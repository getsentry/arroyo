use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};

use super::buffer::Buffer;
use super::triggers::SizeTrigger;
use crate::processing::stream::pipeline_envelope::{MessageMetadata, PipelineEnvelope};
use crate::processing::stream::stage::{FlushableStage, Stage, StageResult};
use crate::backends::kafka::types::KafkaPayload;
use crate::types::Partition;

/// Accumulates items into batches, flushing when either a row count
/// or byte size threshold is reached.
///
/// Uses a `Buffer<T>` for accumulation and two `SizeTrigger`s for
/// completion — one for rows, one for bytes. The buffer's `push()`
/// returns the byte size of each item, which feeds the byte trigger.
///
/// Returns `Skip` while accumulating, `Emit(Vec<T>)` when flushing.
/// Tracks the highest offset per partition across the batch.
pub struct BatchStage<T: Send + Sync, B: Buffer<T>> {
    state: Mutex<BatchState<T, B>>,
    max_rows: u64,
    max_bytes: u64,
}

struct BatchState<T: Send + Sync, B: Buffer<T>> {
    buffer: B,
    row_trigger: SizeTrigger,
    byte_trigger: SizeTrigger,
    offsets: HashMap<Partition, u64>,
    last_metadata: Option<MessageMetadata>,
    last_raw: Option<Arc<KafkaPayload>>,
    _marker: PhantomData<T>,
}

impl<T: Send + Sync, B: Buffer<T>> BatchState<T, B> {
    /// Flush the buffer and reset triggers. Returns `None` if empty.
    fn flush(&mut self, max_rows: u64, max_bytes: u64) -> Option<StageResult<Vec<T>>> {
        if self.buffer.len() == 0 {
            return None;
        }

        let items = self.buffer.flush();
        let mut metadata = self.last_metadata.take()?;
        let raw = self.last_raw.take()?;

        if let Some(&max_offset) = self.offsets.get(&metadata.partition) {
            metadata.offset = max_offset;
        }
        self.offsets.clear();

        self.row_trigger = SizeTrigger::new(max_rows);
        self.byte_trigger = SizeTrigger::new(max_bytes);

        Some(StageResult::Emit(PipelineEnvelope::new(items, metadata, raw)))
    }
}

impl<T: Send + Sync, B: Buffer<T>> BatchStage<T, B> {
    pub fn new(buffer: B, max_rows: u64, max_bytes: u64) -> Self {
        Self {
            state: Mutex::new(BatchState {
                buffer,
                row_trigger: SizeTrigger::new(max_rows),
                byte_trigger: SizeTrigger::new(max_bytes),
                offsets: HashMap::new(),
                last_metadata: None,
                last_raw: None,
                _marker: PhantomData,
            }),
            max_rows,
            max_bytes,
        }
    }
}

impl<T: Send + Sync + 'static, B: Buffer<T> + 'static> Stage for BatchStage<T, B> {
    type In = T;
    type Out = Vec<T>;

    async fn process(&self, envelope: PipelineEnvelope<T>) -> StageResult<Vec<T>> {
        let mut state = self.state.lock().unwrap();

        // Track offsets — keep highest per partition
        state
            .offsets
            .entry(envelope.metadata.partition)
            .and_modify(|o| *o = (*o).max(envelope.metadata.offset))
            .or_insert(envelope.metadata.offset);
        state.last_metadata = Some(envelope.metadata);
        state.last_raw = Some(envelope.raw);

        // Push to buffer and update triggers
        let bytes = state.buffer.push(envelope.payload);
        state.row_trigger.increment(1);
        state.byte_trigger.increment(bytes);

        // Check if either trigger is complete
        if state.row_trigger.is_complete() || state.byte_trigger.is_complete() {
            state.flush(self.max_rows, self.max_bytes)
                .unwrap_or(StageResult::Skip)
        } else {
            StageResult::Skip
        }
    }

    fn name(&self) -> &str {
        "batch"
    }
}

impl<T: Send + Sync + 'static, B: Buffer<T> + 'static> FlushableStage for BatchStage<T, B> {
    fn flush(&self) -> Option<StageResult<Vec<T>>> {
        self.state.lock().unwrap().flush(self.max_rows, self.max_bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::processing::stream::batch::buffer::VecBuffer;
    use crate::processing::stream::{OffsetTracker, PipelineExt};
    use crate::types::Topic;
    use std::time::Duration;

    struct MockCommitter;

    impl crate::processing::stream::OffsetCommitter for MockCommitter {
        fn commit_offsets(
            &self,
            _positions: &HashMap<Partition, u64>,
        ) -> Result<(), Box<dyn std::error::Error + Send>> {
            Ok(())
        }
    }

    fn make_envelope(value: u32, offset: u64) -> StageResult<u32> {
        let kp = KafkaPayload::new(None, None, None);
        let md = MessageMetadata {
            partition: Partition::new(Topic::new("test"), 0),
            offset,
            timestamp: chrono::Utc::now(),
        };
        StageResult::Emit(PipelineEnvelope::new(value, md, Arc::new(kp)))
    }

    struct CollectStage {
        collected: Arc<Mutex<Vec<Vec<u32>>>>,
    }

    impl Stage for CollectStage {
        type In = Vec<u32>;
        type Out = Vec<u32>;

        async fn process(
            &self,
            envelope: PipelineEnvelope<Vec<u32>>,
        ) -> StageResult<Vec<u32>> {
            self.collected
                .lock()
                .unwrap()
                .push(envelope.payload.clone());
            StageResult::Emit(envelope)
        }

        fn name(&self) -> &str {
            "collect"
        }
    }

    // ── Tests ───────────────────────────────────────────────────

    #[tokio::test]
    async fn test_batch_by_row_count() {
        let batch = BatchStage::new(VecBuffer::new(), 3, u64::MAX);
        let collected = Arc::new(Mutex::new(Vec::new()));
        let collector = CollectStage {
            collected: collected.clone(),
        };

        let messages: Vec<_> = (0..7).map(|i| make_envelope(i, i as u64)).collect();
        let committer = MockCommitter;
        let mut tracker = OffsetTracker::new(Duration::from_millis(1), &committer);

        let result = futures::stream::iter(messages)
            .apply(&batch)
            .apply(&collector)
            .commit(&mut tracker)
            .await;

        assert!(result.is_ok());
        let batches = collected.lock().unwrap();
        // 7 items, batch size 3 → 2 batches of 3, 1 item remaining (not flushed)
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0], vec![0, 1, 2]);
        assert_eq!(batches[1], vec![3, 4, 5]);
    }

    #[tokio::test]
    async fn test_batch_by_byte_size() {
        // Custom buffer that reports each item as 10 bytes
        struct TenByteBuffer {
            items: Vec<u32>,
        }
        impl Buffer<u32> for TenByteBuffer {
            fn push(&mut self, item: u32) -> u64 {
                self.items.push(item);
                10
            }
            fn len(&self) -> u64 {
                self.items.len() as u64
            }
            fn flush(&mut self) -> Vec<u32> {
                std::mem::take(&mut self.items)
            }
        }

        // max_bytes=25 → flush after 3 items (30 bytes > 25)
        let batch = BatchStage::new(TenByteBuffer { items: Vec::new() }, u64::MAX, 25);
        let collected = Arc::new(Mutex::new(Vec::new()));
        let collector = CollectStage {
            collected: collected.clone(),
        };

        let messages: Vec<_> = (0..5).map(|i| make_envelope(i, i as u64)).collect();
        let committer = MockCommitter;
        let mut tracker = OffsetTracker::new(Duration::from_millis(1), &committer);

        let result = futures::stream::iter(messages)
            .apply(&batch)
            .apply(&collector)
            .commit(&mut tracker)
            .await;

        assert!(result.is_ok());
        let batches = collected.lock().unwrap();
        // 5 items at 10 bytes each, threshold 25 → flush at 3 items (30 bytes), then 2 remain
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0], vec![0, 1, 2]);
    }

    #[tokio::test]
    async fn test_batch_exact_size() {
        let batch = BatchStage::new(VecBuffer::new(), 3, u64::MAX);
        let collected = Arc::new(Mutex::new(Vec::new()));
        let collector = CollectStage {
            collected: collected.clone(),
        };

        let messages: Vec<_> = (0..6).map(|i| make_envelope(i, i as u64)).collect();
        let committer = MockCommitter;
        let mut tracker = OffsetTracker::new(Duration::from_millis(1), &committer);

        let result = futures::stream::iter(messages)
            .apply(&batch)
            .apply(&collector)
            .commit(&mut tracker)
            .await;

        assert!(result.is_ok());
        let batches = collected.lock().unwrap();
        // 6 items, batch size 3 → exactly 2 batches
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0], vec![0, 1, 2]);
        assert_eq!(batches[1], vec![3, 4, 5]);
    }

    #[tokio::test]
    async fn test_batch_cadence_flush() {
        let batch = BatchStage::new(VecBuffer::new(), 100, u64::MAX);
        let collected = Arc::new(Mutex::new(Vec::new()));
        let collector = CollectStage {
            collected: collected.clone(),
        };

        let stream = async_stream::stream! {
            for i in 0..3u32 {
                yield make_envelope(i, i as u64);
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        };

        let mut noop = crate::processing::stream::NoopCollector;

        let result = stream
            .apply_with_timer(&batch, None, Some(Duration::from_millis(50)))
            .apply(&collector)
            .run(&mut noop)
            .await;

        assert!(result.is_ok());
        let batches = collected.lock().unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0], vec![0, 1, 2]);
    }

    #[tokio::test]
    async fn test_batch_idle_flush() {
        let batch = BatchStage::new(VecBuffer::new(), 100, u64::MAX);
        let collected = Arc::new(Mutex::new(Vec::new()));
        let collector = CollectStage {
            collected: collected.clone(),
        };

        let stream = async_stream::stream! {
            yield make_envelope(0, 0);
            yield make_envelope(1, 1);
            tokio::time::sleep(Duration::from_millis(200)).await;
            yield make_envelope(2, 2);
            yield make_envelope(3, 3);
            tokio::time::sleep(Duration::from_millis(200)).await;
        };

        let mut noop = crate::processing::stream::NoopCollector;

        let result = stream
            .apply_with_timer(&batch, Some(Duration::from_millis(50)), None)
            .apply(&collector)
            .run(&mut noop)
            .await;

        assert!(result.is_ok());
        let batches = collected.lock().unwrap();
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0], vec![0, 1]);
        assert_eq!(batches[1], vec![2, 3]);
    }

    #[tokio::test]
    async fn test_batch_size_trigger_resets_timers() {
        let batch = BatchStage::new(VecBuffer::new(), 2, u64::MAX);
        let collected = Arc::new(Mutex::new(Vec::new()));
        let collector = CollectStage {
            collected: collected.clone(),
        };

        let messages: Vec<_> = (0..4).map(|i| make_envelope(i, i as u64)).collect();

        let mut noop = crate::processing::stream::NoopCollector;

        let result = futures::stream::iter(messages)
            .apply_with_timer(&batch, None, Some(Duration::from_millis(500)))
            .apply(&collector)
            .run(&mut noop)
            .await;

        assert!(result.is_ok());
        let batches = collected.lock().unwrap();
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0], vec![0, 1]);
        assert_eq!(batches[1], vec![2, 3]);
    }

    #[tokio::test]
    async fn test_batch_stream_end_flushes_remainder() {
        let batch = BatchStage::new(VecBuffer::new(), 100, u64::MAX);
        let collected = Arc::new(Mutex::new(Vec::new()));
        let collector = CollectStage {
            collected: collected.clone(),
        };

        let messages: Vec<_> = (0..3).map(|i| make_envelope(i, i as u64)).collect();

        let mut noop = crate::processing::stream::NoopCollector;

        let result = futures::stream::iter(messages)
            .apply_with_timer(&batch, Some(Duration::from_secs(999)), None)
            .apply(&collector)
            .run(&mut noop)
            .await;

        assert!(result.is_ok());
        let batches = collected.lock().unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0], vec![0, 1, 2]);
    }
}
