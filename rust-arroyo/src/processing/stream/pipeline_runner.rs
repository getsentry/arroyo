use std::future::Future;
use std::pin::Pin;

use futures::stream::Stream;

use crate::backends::kafka::types::KafkaPayload;

use super::offset_tracker::OffsetCommitter;
use super::source::PullSource;
use super::stage::{PipelineExit, StageResult};

/// Runs a pipeline in a loop, restarting on rebalance.
///
/// Rebalance flow:
///   1. rdkafka detects partition revocation
///   2. `KafkaSource`'s `ConsumerContext` fires, ending the stream
///   3. Stream yields `StageResult::Exit(Rebalance)`
///   4. `Exit` passes through all combinators to `commit()`
///   5. `commit()` flushes offsets and returns `Ok(PipelineExit::Rebalance)`
///   6. `PipelineRunner` calls the `build` closure again with a fresh stream
///   7. New stream picks up the new partition assignment from rdkafka
///
/// The `build` closure is called once per partition assignment.
/// It receives a fresh stream and committer, builds the pipeline,
/// and returns the exit reason. Create all stages, handlers, and
/// trackers inside the closure so they start fresh each assignment.
///
/// ```ignore
/// PipelineRunner::run(&source, |stream, committer| async move {
///     let stage = MyStage;
///     let mut tracker = OffsetTracker::new(Duration::from_secs(1), committer);
///     stream.apply(&stage).commit(&mut tracker).await
/// }).await?;
/// ```
pub struct PipelineRunner;

impl PipelineRunner {
    pub async fn run<'s, S, F, Fut>(
        source: &'s S,
        mut build: F,
    ) -> Result<(), Box<dyn std::error::Error + Send>>
    where
        S: PullSource,
        F: FnMut(
            Pin<Box<dyn Stream<Item = StageResult<KafkaPayload>> + 's>>,
            &'s dyn OffsetCommitter,
        ) -> Fut,
        Fut: Future<Output = Result<PipelineExit, Box<dyn std::error::Error + Send>>> + 's,
    {
        loop {
            let exit = build(source.stream(), source.committer()).await;
            // Always signal drain complete — unblocks the rebalance callback
            // if one is waiting. No-op if no rebalance is in progress.
            source.signal_drain_complete();
            match exit? {
                PipelineExit::Rebalance => {
                    tracing::info!("Rebalance detected, restarting pipeline");
                    continue;
                }
                PipelineExit::Shutdown | PipelineExit::Complete => return Ok(()),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, VecDeque};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use super::*;
    use crate::backends::kafka::types::KafkaPayload;
    use crate::processing::stream::{
        MessageMetadata, OffsetTracker, PipelineEnvelope, PipelineExt,
    };
    use crate::types::{Partition, Topic};

    struct MockCommitter {
        committed: Mutex<Vec<HashMap<Partition, u64>>>,
    }

    impl MockCommitter {
        fn new() -> Self {
            Self {
                committed: Mutex::new(Vec::new()),
            }
        }

        fn commit_count(&self) -> usize {
            self.committed.lock().unwrap().len()
        }
    }

    impl OffsetCommitter for MockCommitter {
        fn commit_offsets(
            &self,
            positions: &HashMap<Partition, u64>,
        ) -> Result<(), Box<dyn std::error::Error + Send>> {
            self.committed.lock().unwrap().push(positions.clone());
            Ok(())
        }
    }

    /// Test source that yields pre-configured batches of messages.
    /// Each call to `stream()` pops the next batch and exit reason.
    struct RebalanceTestSource {
        batches: Mutex<VecDeque<(Vec<StageResult<KafkaPayload>>, PipelineExit)>>,
        committer: MockCommitter,
    }

    impl RebalanceTestSource {
        fn new(batches: Vec<(Vec<StageResult<KafkaPayload>>, PipelineExit)>) -> Self {
            Self {
                batches: Mutex::new(VecDeque::from(batches)),
                committer: MockCommitter::new(),
            }
        }

        fn commit_count(&self) -> usize {
            self.committer.commit_count()
        }
    }

    impl PullSource for RebalanceTestSource {
        fn stream(&self) -> Pin<Box<dyn Stream<Item = StageResult<KafkaPayload>> + '_>> {
            let (messages, exit) = self
                .batches
                .lock()
                .unwrap()
                .pop_front()
                .expect("RebalanceTestSource: no more batches");
            Box::pin(async_stream::stream! {
                for msg in messages {
                    yield msg;
                }
                yield StageResult::Exit(exit);
            })
        }

        fn committer(&self) -> &dyn OffsetCommitter {
            &self.committer
        }

        fn shutdown(&self) {}
        fn signal_drain_complete(&self) {}
    }

    fn make_message(payload: &[u8], offset: u64) -> StageResult<KafkaPayload> {
        let kp = KafkaPayload::new(None, None, Some(payload.to_vec()));
        let md = MessageMetadata {
            partition: Partition::new(Topic::new("test"), 0),
            offset,
            timestamp: chrono::Utc::now(),
        };
        StageResult::Emit(PipelineEnvelope::new(kp.clone(), md, Arc::new(kp)))
    }

    #[tokio::test]
    async fn test_pipeline_runner_rebalance() {
        let source = RebalanceTestSource::new(vec![
            // First assignment: 2 messages, then rebalance
            (
                vec![make_message(b"a", 0), make_message(b"b", 1)],
                PipelineExit::Rebalance,
            ),
            // Second assignment: 2 messages, then shutdown
            (
                vec![make_message(b"c", 0), make_message(b"d", 1)],
                PipelineExit::Shutdown,
            ),
        ]);

        let call_count = AtomicUsize::new(0);

        let result = PipelineRunner::run(&source, |stream, committer| {
            call_count.fetch_add(1, Ordering::SeqCst);
            async move {
                let mut tracker = OffsetTracker::new(Duration::from_millis(1), committer);
                stream.commit(&mut tracker).await
            }
        })
        .await;

        assert!(result.is_ok());
        assert_eq!(
            call_count.load(Ordering::SeqCst),
            2,
            "Closure should be called twice"
        );
        assert!(source.commit_count() > 0, "Offsets should be committed");
    }

    #[tokio::test]
    async fn test_pipeline_runner_shutdown() {
        let source = RebalanceTestSource::new(vec![(
            vec![make_message(b"a", 0), make_message(b"b", 1)],
            PipelineExit::Shutdown,
        )]);

        let call_count = AtomicUsize::new(0);

        let result = PipelineRunner::run(&source, |stream, committer| {
            call_count.fetch_add(1, Ordering::SeqCst);
            async move {
                let mut tracker = OffsetTracker::new(Duration::from_millis(1), committer);
                stream.commit(&mut tracker).await
            }
        })
        .await;

        assert!(result.is_ok());
        assert_eq!(
            call_count.load(Ordering::SeqCst),
            1,
            "Closure should be called once"
        );
    }

    #[tokio::test]
    async fn test_pipeline_runner_complete() {
        // Source that naturally ends (no Exit item)
        struct FiniteSource {
            committer: MockCommitter,
        }

        impl PullSource for FiniteSource {
            fn stream(&self) -> Pin<Box<dyn Stream<Item = StageResult<KafkaPayload>> + '_>> {
                let messages = vec![make_message(b"a", 0), make_message(b"b", 1)];
                Box::pin(futures::stream::iter(messages))
            }
            fn committer(&self) -> &dyn OffsetCommitter {
                &self.committer
            }
            fn shutdown(&self) {}
            fn signal_drain_complete(&self) {}
        }

        let source = FiniteSource {
            committer: MockCommitter::new(),
        };

        let result = PipelineRunner::run(&source, |stream, committer| async move {
            let mut tracker = OffsetTracker::new(Duration::from_millis(1), committer);
            stream.commit(&mut tracker).await
        })
        .await;

        assert!(result.is_ok());
    }
}
