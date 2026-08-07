use std::time::Instant;

use futures::stream::Stream;
use futures::StreamExt;

use super::offset_tracker::OffsetTracker;
use crate::{counter, timer};

use super::handlers::rejection::{RejectionMetadata, RejectionHandler};
use super::handlers::next::NextHandler;
use super::pipeline_envelope::PipelineEnvelope;
use super::stage::{Stage, StageResult};

/// Run a stage on an envelope and record metrics.
async fn run_stage<S: Stage>(
    stage: &S,
    envelope: PipelineEnvelope<S::In>,
) -> StageResult<S::Out> {
    let start = Instant::now();
    let result = stage.process(envelope).await;

    timer!("arroyo.stage.duration", start.elapsed(), "stage" => stage.name());
    match &result {
        StageResult::Emit(_) => { counter!("arroyo.stage.success", 1, "stage" => stage.name()); }
        StageResult::Drop { .. } => { counter!("arroyo.stage.drop", 1, "stage" => stage.name()); }
        StageResult::Skip => { counter!("arroyo.stage.skip", 1, "stage" => stage.name()); }
        StageResult::Reject { .. } => { counter!("arroyo.stage.reject", 1, "stage" => stage.name()); }
        StageResult::Fail(_) => { counter!("arroyo.stage.fail", 1, "stage" => stage.name()); }
    }

    result
}

/// Extension trait that adds pipeline combinators to any
/// Stream<Item = StageResult<T>>.
///
/// Usage:
///   source.stream()
///       .apply(&parse)
///       .apply(&transform)
///       .apply_concurrent(&ch_writer, 8)
///       .on_next(&produce_handler)
///       .on_reject(&dlq_handler)
///       .commit(&mut tracker)
///       .await;
pub trait PipelineExt<T: Send>:
    Stream<Item = StageResult<T>> + Sized
{
    /// Apply a processing stage sequentially to each Emit envelope.
    /// Equivalent to apply_concurrent(stage, 1).
    fn apply<'a, S>(
        self,
        stage: &'a S,
    ) -> impl Stream<Item = StageResult<S::Out>> + 'a
    where
        S: Stage<In = T>,
        Self: 'a,
        T: 'a,
    {
        self.apply_concurrent(stage, 1)
    }

    /// Apply a processing stage concurrently to up to `concurrency` Emit
    /// envelopes at once. Results are yielded in input order.
    /// Non-Emit items (Drop, Skip, Reject, Fail) resolve immediately.
    /// Records per-stage metrics.
    fn apply_concurrent<'a, S>(
        self,
        stage: &'a S,
        concurrency: usize,
    ) -> impl Stream<Item = StageResult<S::Out>> + 'a
    where
        S: Stage<In = T>,
        Self: 'a,
        T: 'a,
    {
        self.map(move |item| async move {
            match item {
                StageResult::Emit(e) => run_stage(stage, e).await,
                StageResult::Drop { metadata } => StageResult::Drop { metadata },
                StageResult::Skip => StageResult::Skip,
                StageResult::Reject { metadata, raw, reason } => {
                    StageResult::Reject { metadata, raw, reason }
                }
                StageResult::Fail(err) => StageResult::Fail(err),
            }
        })
        .buffered(concurrency)
    }

    /// Call the next handler for each Emit envelope.
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
        tracker: &mut OffsetTracker<'_>,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        let mut stream = Box::pin(self);

        while let Some(item) = stream.next().await {
            match item {
                StageResult::Emit(envelope) => {
                    tracker.track(envelope.metadata.partition, envelope.metadata.offset + 1);
                    tracker.record_latency(envelope.metadata.timestamp);
                }
                StageResult::Drop { metadata } => {
                    tracker.track(metadata.partition, metadata.offset + 1);
                }
                StageResult::Skip => {}
                StageResult::Reject { metadata, .. } => {
                    tracker.track(metadata.partition, metadata.offset + 1);
                }
                StageResult::Fail(err) => {
                    let _ = tracker.flush();
                    return Err(err);
                }
            }

            tracker.maybe_commit()?;
        }

        tracker.flush()?;
        Ok(())
    }
}

// Blanket impl: any Stream of StageResult<T> gets these methods.
impl<T: Send, S> PipelineExt<T> for S where S: Stream<Item = StageResult<T>> {}
