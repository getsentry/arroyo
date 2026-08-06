use std::time::Instant;

use futures::stream::Stream;
use futures::StreamExt;

use crate::processing::strategies::offset_tracker::OffsetTracker;
use crate::{counter, timer};

use super::handlers::rejection::{RejectionMetadata, RejectionHandler};
use super::handlers::next::NextHandler;
use super::stage::{Stage, StageResult};

/// Extension trait that adds pipeline combinators to any
/// Stream<Item = StageResult<T>>.
///
/// Usage:
///   source.stream()
///       .apply_stage(&parse)
///       .apply_stage(&transform)
///       .on_ok(&produce_handler)
///       .on_reject(&dlq_handler)
///       .commit(&mut tracker)
///       .await;
pub trait PipelineExt<T: Send>:
    Stream<Item = StageResult<T>> + Sized
{
    /// Apply a processing stage to each Emit envelope in the stream.
    /// Drop and Skip are filtered out of the stream (but Drop is passed
    /// through for offset tracking). Reject and Fail pass through unchanged.
    /// Records per-stage metrics (duration, success/failure counts).
    fn apply_stage<'a, S>(
        self,
        stage: &'a S,
    ) -> impl Stream<Item = StageResult<S::Out>> + 'a
    where
        S: Stage<In = T>,
        Self: 'a,
        T: 'a,
    {
        self.filter_map(move |item| async move {
            let envelope = match item {
                StageResult::Emit(e) => e,
                StageResult::Drop { metadata } => {
                    return Some(StageResult::Drop { metadata });
                }
                StageResult::Skip => return None,
                StageResult::Reject { metadata, raw, reason } => {
                    return Some(StageResult::Reject { metadata, raw, reason });
                }
                StageResult::Fail(err) => return Some(StageResult::Fail(err)),
            };

            let start = Instant::now();
            let result = stage.process(envelope).await;

            timer!("arroyo.stage.duration", start.elapsed(), "stage" => stage.name());
            match &result {
                StageResult::Emit(_) => {
                    counter!("arroyo.stage.success", 1, "stage" => stage.name());
                }
                StageResult::Drop { .. } => {
                    counter!("arroyo.stage.drop", 1, "stage" => stage.name());
                }
                StageResult::Skip => {
                    counter!("arroyo.stage.skip", 1, "stage" => stage.name());
                }
                StageResult::Reject { .. } => {
                    counter!("arroyo.stage.reject", 1, "stage" => stage.name());
                }
                StageResult::Fail(_) => {
                    counter!("arroyo.stage.fail", 1, "stage" => stage.name());
                }
            }

            Some(result)
        })
    }

    /// Call the success handler for each Emit envelope.
    /// If the handler fails, the item becomes Fail.
    /// All other variants pass through untouched.
    fn on_next<'a, H>(
        self,
        handler: &'a H,
    ) -> impl Stream<Item = StageResult<T>> + 'a
    where
        H: NextHandler<T>,
        Self: 'a,
        T: 'a,
    {
        self.then(move |item| async move {
            let envelope = match item {
                StageResult::Emit(e) => e,
                other => return other,
            };

            match handler.handle(&envelope).await {
                Ok(()) => StageResult::Emit(envelope),
                Err(produce_err) => StageResult::Fail(produce_err),
            }
        })
    }

    /// Call the rejection handler for each Reject item.
    /// All other variants pass through untouched.
    /// If the handler itself fails, the item becomes Fail.
    fn on_reject<'a, H>(
        self,
        handler: &'a H,
    ) -> impl Stream<Item = StageResult<T>> + 'a
    where
        H: RejectionHandler,
        Self: 'a,
        T: 'a,
    {
        self.then(move |item| async move {
            match item {
                StageResult::Reject { metadata, raw, reason } => {
                    let rejected = RejectionMetadata {
                        metadata: metadata.clone(),
                        raw: raw.clone(),
                        reason,
                    };

                    match handler.handle(&rejected).await {
                        Ok(()) => {
                            // Handled (e.g. DLQ'd). Keep as Reject so commit
                            // can still track the offset.
                            StageResult::Reject { metadata, raw, reason }
                        }
                        Err(handler_err) => StageResult::Fail(handler_err),
                    }
                }
                other => other,
            }
        })
    }

    /// Terminal: drive the pipeline to completion.
    /// Tracks offsets for Emit, Drop, and Reject items. Fail stops the pipeline.
    /// Skip items pass through without offset tracking (batch will emit later).
    #[allow(async_fn_in_trait)]
    async fn commit(
        self,
        tracker: &mut OffsetTracker,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        let mut stream = Box::pin(self);

        while let Some(item) = stream.next().await {
            match item {
                StageResult::Emit(envelope) => {
                    tracker.track(envelope.metadata.partition, envelope.metadata.offset + 1);
                    tracker.record_latency(envelope.metadata.timestamp);
                }
                StageResult::Drop { metadata } => {
                    // Filtered — track offset so we advance past it.
                    tracker.track(metadata.partition, metadata.offset + 1);
                }
                StageResult::Skip => {
                    // Batching — offset will be tracked when the batch emits.
                }
                StageResult::Reject { metadata, .. } => {
                    // Already handled by on_reject — track the offset.
                    tracker.track(metadata.partition, metadata.offset + 1);
                }
                StageResult::Fail(err) => {
                    if let Some(_positions) = tracker.flush() {
                        // TODO: actually commit via consumer
                    }
                    return Err(err);
                }
            }

            if let Some(_positions) = tracker.maybe_commit() {
                // TODO: actually commit via consumer
            }
        }

        if let Some(_positions) = tracker.flush() {
            // TODO: actually commit via consumer
        }

        Ok(())
    }
}

// Blanket impl: any Stream of StageResult<T> gets these methods.
impl<T: Send, S> PipelineExt<T> for S where S: Stream<Item = StageResult<T>> {}
