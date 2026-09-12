use std::time::Duration;

use super::offset_tracker::OffsetTracker;
use super::pipeline::Pipeline;
use super::source::PullSource;
use super::stage::PipelineExit;
use super::{BoxError, PipelineExt};

/// Runs a pipeline in a loop, restarting on rebalance.
///
/// Rebalance flow:
///   1. rdkafka detects partition revocation
///   2. `KafkaSource`'s `ConsumerContext` fires, ending the stream
///   3. Stream yields `StageResult::Exit(Rebalance)`
///   4. `Exit` passes through all combinators to `commit()`
///   5. `commit()` flushes offsets and returns `Ok(PipelineExit::Rebalance)`
///   6. `PipelineRunner` calls the build closure again with a fresh pipeline
///   7. New stream picks up the new partition assignment from rdkafka
pub struct PipelineRunner;

impl PipelineRunner {
    /// Run a `Pipeline` with automatic rebalance handling.
    ///
    /// The `build` closure is called once per partition assignment.
    /// It should create a fresh pipeline with new per-run state
    /// (batch buffers, timers). Shared resources (producers, writers)
    /// should be captured by the closure via `Arc`.
    ///
    /// ```ignore
    /// PipelineRunner::run_pipeline(&source, Duration::from_secs(1), || {
    ///     MyPipeline::new(shared.clone(), processing_concurrency)
    /// }).await?;
    /// ```
    pub async fn run<S, F, P>(
        source: &S,
        commit_interval: Duration,
        build: F,
    ) -> Result<(), BoxError>
    where
        S: PullSource,
        F: Fn() -> P,
        P: Pipeline,
    {
        loop {
            let pipeline = build();
            let mut tracker = OffsetTracker::new(commit_interval, source.committer());

            let result = pipeline
                .stream(source.stream())
                .commit(&mut tracker)
                .await?;

            match result {
                PipelineExit::Rebalance => {
                    tracing::info!("Rebalance detected, restarting pipeline...");
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
    use std::sync::Mutex;
    use std::time::Duration;

    use futures::Stream;

    use super::*;
    use crate::backends::kafka::types::KafkaPayload;
    use crate::processing::stream::{
        BoxStream, MessageMetadata, OffsetCommitter, PipelineEnvelope, StageResult,
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
        fn commit_offsets(&self, positions: &HashMap<Partition, u64>) -> Result<(), BoxError> {
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
        fn stream(&self) -> BoxStream<'_, StageResult<KafkaPayload>> {
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
    }

    fn make_message(payload: &[u8], offset: u64) -> StageResult<KafkaPayload> {
        let kp = KafkaPayload::new(None, None, Some(payload.to_vec()));
        let md = MessageMetadata {
            partition: Partition::new(Topic::new("test"), 0),
            offset,
            timestamp: chrono::Utc::now(),
        };
        StageResult::Emit(PipelineEnvelope::new(kp.clone(), md, kp))
    }

    /// Identity pipeline — passes messages through unchanged.
    struct IdentityPipeline;

    impl Pipeline for IdentityPipeline {
        type Output = KafkaPayload;

        fn stream(
            self,
            source: impl Stream<Item = StageResult<KafkaPayload>> + Send,
        ) -> impl Stream<Item = StageResult<KafkaPayload>> + Send {
            source
        }
    }

    #[tokio::test]
    async fn test_pipeline_runner_rebalance() {
        let source = RebalanceTestSource::new(vec![
            (
                vec![make_message(b"a", 0), make_message(b"b", 1)],
                PipelineExit::Rebalance,
            ),
            (
                vec![make_message(b"c", 0), make_message(b"d", 1)],
                PipelineExit::Shutdown,
            ),
        ]);

        let call_count = AtomicUsize::new(0);

        let result = PipelineRunner::run(&source, Duration::from_millis(1), || {
            call_count.fetch_add(1, Ordering::SeqCst);
            IdentityPipeline
        })
        .await;

        assert!(result.is_ok());
        assert_eq!(
            call_count.load(Ordering::SeqCst),
            2,
            "Build should be called twice"
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

        let result = PipelineRunner::run(&source, Duration::from_millis(1), || {
            call_count.fetch_add(1, Ordering::SeqCst);
            IdentityPipeline
        })
        .await;

        assert!(result.is_ok());
        assert_eq!(
            call_count.load(Ordering::SeqCst),
            1,
            "Build should be called once"
        );
    }

    #[tokio::test]
    async fn test_pipeline_runner_complete() {
        struct FiniteSource {
            committer: MockCommitter,
        }

        impl PullSource for FiniteSource {
            fn stream(&self) -> BoxStream<'_, StageResult<KafkaPayload>> {
                let messages = vec![make_message(b"a", 0), make_message(b"b", 1)];
                Box::pin(futures::stream::iter(messages))
            }
            fn committer(&self) -> &dyn OffsetCommitter {
                &self.committer
            }
            fn shutdown(&self) {}
        }

        let source = FiniteSource {
            committer: MockCommitter::new(),
        };

        let result =
            PipelineRunner::run(&source, Duration::from_millis(1), || IdentityPipeline).await;

        assert!(result.is_ok());
    }
}
