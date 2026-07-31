use std::time::Instant;

use futures::stream::Stream;
use futures::StreamExt;

use crate::processing::strategies::offset_tracker::OffsetTracker;
use crate::{counter, timer};

use super::envelope::Envelope;
use super::error_handler::{ErrorContext, ErrorHandler, ErrorKind};
use super::stage::{Stage, StageError};
use super::success_handler::SuccessHandler;

/// Extension trait that adds pipeline combinators to any
/// Stream<Item = Result<Envelope<T>, StageError>>.
///
/// Usage:
///   source.stream()
///       .apply_stage(&parse)
///       .apply_stage(&transform)
///       .on_ok(&produce_handler)
///       .on_error(&dlq_handler)
///       .commit(&mut tracker)
///       .await;
pub trait PipelineExt<T: Send>:
    Stream<Item = Result<Envelope<T>, StageError>> + Sized
{
    /// Apply a processing stage to each Ok envelope in the stream.
    /// Err values pass through untouched (railway pattern).
    /// Records per-stage metrics (duration, success/failure counts).
    fn apply_stage<'a, S>(
        self,
        stage: &'a S,
    ) -> impl Stream<Item = Result<Envelope<S::Out>, StageError>> + 'a
    where
        S: Stage<In = T>,
        Self: 'a,
        T: 'a,
    {
        self.then(move |result| async move {
            let envelope = match result {
                Ok(e) => e,
                Err(e) => return Err(e),
            };

            let start = Instant::now();
            let result = stage.process(envelope).await;

            timer!("arroyo.stage.duration", start.elapsed(), "stage" => stage.name());
            match &result {
                Ok(_) => { counter!("arroyo.stage.success", 1, "stage" => stage.name()); }
                Err(StageError::Invalid { .. }) => {
                    counter!("arroyo.stage.invalid", 1, "stage" => stage.name());
                }
                Err(StageError::Fatal(_)) => {
                    counter!("arroyo.stage.fatal", 1, "stage" => stage.name());
                }
            }

            result
        })
    }

    /// Call the success handler for each Ok envelope.
    /// If the handler fails, convert the Ok into an Err with ErrorKind::ProduceFailure.
    /// Err values pass through untouched.
    fn on_ok<'a, H>(
        self,
        handler: &'a H,
    ) -> impl Stream<Item = Result<Envelope<T>, StageError>> + 'a
    where
        H: SuccessHandler<T>,
        Self: 'a,
        T: 'a,
    {
        self.then(move |result| async move {
            let envelope = match result {
                Ok(e) => e,
                Err(e) => return Err(e),
            };

            match handler.handle(&envelope).await {
                Ok(()) => Ok(envelope),
                Err(produce_err) => {
                    // Produce failed — fatal, same as current behavior.
                    Err(StageError::Fatal(produce_err))
                }
            }
        })
    }

    /// Call the error handler for each Err in the stream.
    /// Ok values pass through untouched.
    /// If the error handler itself fails, the item becomes a Fatal error.
    fn on_error<'a, H>(
        self,
        handler: &'a H,
    ) -> impl Stream<Item = Result<Envelope<T>, StageError>> + 'a
    where
        H: ErrorHandler,
        Self: 'a,
        T: 'a,
    {
        self.then(move |result| async move {
            match result {
                Ok(envelope) => Ok(envelope),
                Err(StageError::Invalid { origin, reason }) => {
                    let error_context = ErrorContext {
                        origin: origin.clone(),
                        source: Box::new(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("Invalid message: {:?}", reason),
                        )),
                        kind: ErrorKind::InvalidMessage,
                    };

                    match handler.handle(error_context).await {
                        Ok(()) => {
                            // Error was handled (e.g. DLQ'd). Keep the Err in the
                            // stream so commit() can still track the offset.
                            Err(StageError::Invalid { origin, reason })
                        }
                        Err(handler_err) => Err(StageError::Fatal(handler_err)),
                    }
                }
                Err(StageError::Fatal(err)) => Err(StageError::Fatal(err)),
            }
        })
    }

    /// Terminal: drive the pipeline to completion.
    /// Tracks offsets for every item (Ok and Err) and commits on the
    /// tracker's configured interval. Fatal errors stop the pipeline.
    async fn commit(
        self,
        tracker: &mut OffsetTracker,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        let mut stream = Box::pin(self);

        while let Some(result) = stream.next().await {
            match result {
                Ok(envelope) => {
                    tracker.track(envelope.partition(), envelope.offset() + 1);
                    tracker.record_latency(envelope.timestamp());
                }
                Err(StageError::Invalid { origin, .. }) => {
                    // Already handled by on_error — just track the offset.
                    tracker.track(origin.partition, origin.offset + 1);
                }
                Err(StageError::Fatal(err)) => {
                    // Flush any pending offsets before stopping.
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

        // Stream ended — flush final offsets.
        if let Some(_positions) = tracker.flush() {
            // TODO: actually commit via consumer
        }

        Ok(())
    }
}

// Blanket impl: any Stream of Result<Envelope<T>, StageError> gets these methods.
impl<T: Send, S> PipelineExt<T> for S where
    S: Stream<Item = Result<Envelope<T>, StageError>>
{
}
