use std::sync::Arc;
use std::time::Duration;

use futures::stream::Stream;
use futures::StreamExt;

use super::collector::StreamCollector;
use super::offset_tracker::OffsetTracker;
use crate::processing::stream::batch::flush_timer::FlushTimer;
use crate::{counter, timer};

use super::handlers::next::NextHandler;
use super::handlers::rejection::{RejectionHandler, RejectionMetadata};
use super::pipeline_envelope::PipelineEnvelope;
use super::stage::{FlushableStage, PipelineExit, Stage, StageResult};
use super::BoxError;

/// Run a stage on an envelope and record metrics.
async fn run_stage<S: Stage>(stage: &S, envelope: PipelineEnvelope<S::In>) -> StageResult<S::Out> {
    let start = coarsetime::Instant::now();
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
        StageResult::Exit(_) => {}
    }

    result
}

/// Extension trait that adds pipeline combinators to any
/// Stream<Item = StageResult<T>>.
///
/// Combinators: `.apply()`, `.apply_concurrent()`, `.on_next()`,
/// `.on_reject()`, `.commit()`.
///
/// See `PipelineRunner` for the recommended way to run a pipeline
/// with rebalance handling.
pub trait PipelineExt<T: Send>: Stream<Item = StageResult<T>> + Sized {
    /// Apply a processing stage sequentially to each Emit envelope.
    fn apply<S>(self, stage: S) -> impl Stream<Item = StageResult<S::Out>>
    where
        S: Stage<In = T>,
    {
        async_stream::stream! {
            let mut upstream = Box::pin(self);
            while let Some(item) = upstream.next().await {
                yield item.map_emit(|e| run_stage(&stage, e)).await;
            }
        }
    }

    /// Apply a processing stage concurrently to up to `concurrency` Emit
    /// envelopes at once. Results are yielded in input order.
    /// Non-Emit items pass through immediately.
    fn apply_concurrent<S>(
        self,
        stage: S,
        concurrency: usize,
    ) -> impl Stream<Item = StageResult<S::Out>>
    where
        S: Stage<In = T>,
    {
        assert!(concurrency > 0, "concurrency must be at least 1");
        let stage = Arc::new(stage);
        self.map(move |item| {
            let stage = Arc::clone(&stage);
            async move { item.map_emit(|e| run_stage(&*stage, e)).await }
        })
        .buffered(concurrency)
    }

    /// Apply a flushable stage with time-based flush triggers.
    ///
    /// Two optional timers:
    /// - `idle_timeout`: flush if no upstream item arrives within this duration.
    /// - `max_cadence`: flush at this interval since the batch started accumulating.
    ///
    /// At least one of `idle_timeout` or `max_cadence` must be `Some`.
    fn apply_with_timer<S>(
        self,
        stage: S,
        idle_timeout: Option<Duration>,
        max_cadence: Option<Duration>,
    ) -> impl Stream<Item = StageResult<S::Out>>
    where
        S: FlushableStage<In = T>,
    {
        assert!(
            idle_timeout.is_some() || max_cadence.is_some(),
            "apply_with_timer requires at least one of idle_timeout or max_cadence"
        );

        async_stream::stream! {
            let mut upstream = Box::pin(self);
            let mut timer = FlushTimer::new(idle_timeout, max_cadence);

            loop {
                tokio::select! {
                    item = upstream.next() => {
                        let item = match item {
                            Some(item) => item,
                            None => {
                                if let Some(flushed) = stage.flush() {
                                    yield flushed;
                                }
                                return;
                            }
                        };

                        match item {
                            StageResult::Emit(e) => {
                                let result = run_stage(&stage, e).await;
                                match &result {
                                    StageResult::Skip => timer.on_accumulate(),
                                    StageResult::Emit(_) => timer.on_flush(),
                                    _ => {}
                                }
                                yield result;
                            }
                            StageResult::Exit(reason) => {
                                if let Some(flushed) = stage.flush() {
                                    yield flushed;
                                }
                                yield StageResult::Exit(reason);
                                return;
                            }
                            StageResult::Fail(err) => {
                                yield StageResult::Fail(err);
                                return;
                            }
                            StageResult::Drop { metadata } => {
                                yield StageResult::Drop { metadata };
                            }
                            StageResult::Skip => yield StageResult::Skip,
                            StageResult::Reject { metadata, raw, reason } => {
                                yield StageResult::Reject { metadata, raw, reason };
                            }
                        }
                    }
                    _ = timer.interval.tick(), if timer.is_active() => {
                        if timer.should_flush() {
                            if let Some(flushed) = stage.flush() {
                                yield flushed;
                            }
                            timer.on_flush();
                        }
                    }
                }
            }
        }
    }

    /// Call the next handler for each Emit envelope.
    /// If the handler fails, the item becomes Fail.
    /// All other variants pass through untouched.
    fn on_next<H>(self, handler: H) -> impl Stream<Item = StageResult<T>>
    where
        H: NextHandler<T>,
    {
        async_stream::stream! {
            let mut upstream = Box::pin(self);
            while let Some(item) = upstream.next().await {
                let envelope = match item {
                    StageResult::Emit(e) => e,
                    other => {
                        yield other;
                        continue;
                    }
                };
                match handler.handle(&envelope).await {
                    Ok(()) => yield StageResult::Emit(envelope),
                    Err(produce_err) => yield StageResult::Fail(produce_err),
                }
            }
        }
    }

    /// Call the rejection handler for each Reject item.
    /// All other variants pass through untouched.
    /// If the handler itself fails, the item becomes Fail.
    fn on_reject<H>(self, handler: H) -> impl Stream<Item = StageResult<T>>
    where
        H: RejectionHandler,
    {
        async_stream::stream! {
            let mut upstream = Box::pin(self);
            while let Some(item) = upstream.next().await {
                match item {
                    StageResult::Reject { metadata, raw, reason } => {
                        let rejected = RejectionMetadata {
                            metadata: metadata.clone(),
                            raw: raw.clone(),
                            reason,
                        };
                        match handler.handle(&rejected).await {
                            Ok(()) => yield StageResult::Reject { metadata, raw, reason },
                            Err(handler_err) => yield StageResult::Fail(handler_err),
                        }
                    }
                    other => yield other,
                }
            }
        }
    }

    /// Terminal: drive the pipeline to completion.
    /// Tracks offsets for Emit, Drop, and Reject items.
    /// Returns the exit reason (Rebalance, Shutdown, or Complete).
    /// Fail stops the pipeline with an error.
    #[allow(async_fn_in_trait)]
    async fn commit(self, tracker: &mut OffsetTracker<'_>) -> Result<PipelineExit, BoxError> {
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
                StageResult::Exit(reason) => {
                    tracker.flush()?;
                    return Ok(reason);
                }
            }

            tracker.maybe_commit()?;
        }

        // Stream ended naturally (no Exit item)
        tracker.flush()?;
        Ok(PipelineExit::Complete)
    }

    /// Terminal: drive the pipeline to completion using a StreamCollector.
    ///
    /// Like `commit()`, but delegates event handling to the collector.
    /// Use `OffsetCollector` for production (offset tracking + commit),
    /// or `NoopCollector` / a custom collector for tests.
    #[allow(async_fn_in_trait)]
    async fn run<C: StreamCollector<T>>(self, collector: &mut C) -> Result<PipelineExit, BoxError> {
        let mut stream = Box::pin(self);

        while let Some(item) = stream.next().await {
            match item {
                StageResult::Emit(envelope) => {
                    collector.on_emit(&envelope);
                }
                StageResult::Drop { metadata } => {
                    collector.on_drop(&metadata);
                }
                StageResult::Skip => {}
                StageResult::Reject { metadata, .. } => {
                    collector.on_reject(&metadata);
                }
                StageResult::Fail(err) => {
                    let _ = collector.on_complete();
                    return Err(err);
                }
                StageResult::Exit(reason) => {
                    collector.on_complete()?;
                    return Ok(reason);
                }
            }
        }

        collector.on_complete()?;
        Ok(PipelineExit::Complete)
    }
}

// Blanket impl: any Stream of StageResult<T> gets these methods.
impl<T: Send, S> PipelineExt<T> for S where S: Stream<Item = StageResult<T>> {}
