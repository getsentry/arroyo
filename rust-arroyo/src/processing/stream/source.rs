use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

use async_stream::stream;
use futures::stream::Stream;
use futures::StreamExt;
use rdkafka::config::ClientConfig as RdKafkaConfig;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer, ConsumerContext, StreamConsumer};
use rdkafka::types::RDKafkaRespErr;
use rdkafka::{ClientContext, TopicPartitionList};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use crate::backends::kafka::config::KafkaConfig;
use crate::backends::kafka::kafka_poll_error_is_recoverable;
use crate::backends::kafka::types::KafkaPayload;
use crate::types::{Partition, Topic};

use super::offset_tracker::OffsetCommitter;
use super::pipeline_envelope::PipelineEnvelope;
use super::stage::{PipelineExit, StageResult};

/// Trait for pipeline sources. Provides a stream of raw Kafka payloads,
/// an offset committer, and graceful shutdown.
///
/// Object-safe — can be used as `Box<dyn PullSource>` or `Arc<dyn PullSource>`.
pub trait PullSource: Send + Sync {
    /// Returns a stream of Kafka messages. Ends on rebalance, shutdown,
    /// or when the source is exhausted. Callable multiple times — each
    /// call returns a fresh stream for the current partition assignment.
    fn stream(&self) -> Pin<Box<dyn Stream<Item = StageResult<KafkaPayload>> + '_>>;

    /// Returns the offset committer for this source.
    fn committer(&self) -> &dyn OffsetCommitter;

    /// Initiate graceful shutdown. The stream will yield Exit(Shutdown).
    fn shutdown(&self);
}

/// ConsumerContext that signals the pipeline on partition revocation.
///
/// NOTE: The rebalance callback runs inline during StreamConsumer::poll_next
/// (same thread/task as the async stream). We cannot block here to drain
/// the pipeline — that would deadlock. Instead, we just notify and return.
/// rdkafka proceeds with unassign immediately. The pipeline drains after
/// unassign, so offset commits are best-effort.
///
/// For drain-before-unassign, we would need to switch to BaseConsumer
/// with manual polling (like the push model does).
struct PullRebalanceContext {
    revoke: Arc<Notify>,
}

impl ClientContext for PullRebalanceContext {}

impl ConsumerContext for PullRebalanceContext {
    fn rebalance(
        &self,
        base_consumer: &BaseConsumer<Self>,
        err: RDKafkaRespErr,
        tpl: &mut TopicPartitionList,
    ) {
        if err == RDKafkaRespErr::RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS {
            tracing::info!("Partition revocation detected");
            self.revoke.notify_one();
            base_consumer
                .unassign()
                .expect("Failed to unassign partitions");
        } else if err == RDKafkaRespErr::RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS {
            tracing::info!("Partition assignment received");
            // TODO: The push model explicitly fetches committed offsets from
            // the broker and resolves unset offsets via InitialOffset +
            // watermarks before assigning. We rely on rdkafka's default
            // behavior (auto.offset.reset). Add explicit resolution if
            // edge cases arise.
            base_consumer
                .assign(tpl)
                .expect("Failed to assign partitions");
        }
    }
}

/// A Kafka consumer source that produces a Stream of StageResult<KafkaPayload>.
///
/// Handles three lifecycle events:
/// - Messages: yielded as StageResult::Emit
/// - Rebalance: yields StageResult::Exit(Rebalance), then ends
/// - Shutdown: yields StageResult::Exit(Shutdown), then ends
///
/// The stream can be called again after a rebalance — the underlying
/// StreamConsumer is reused with the new partition assignment.
pub struct KafkaSource {
    consumer: StreamConsumer<PullRebalanceContext>,
    shutdown: CancellationToken,
    revoke: Arc<Notify>,
}

impl KafkaSource {
    pub fn new(config: KafkaConfig, topics: &[Topic]) -> Self {
        let revoke = Arc::new(Notify::new());
        let context = PullRebalanceContext {
            revoke: revoke.clone(),
        };

        let mut rdkafka_config: RdKafkaConfig = config.into();
        let consumer: StreamConsumer<PullRebalanceContext> = rdkafka_config
            .set_log_level(rdkafka::config::RDKafkaLogLevel::Warning)
            .create_with_context(context)
            .expect("Failed to create consumer");

        let topic_strs: Vec<&str> = topics.iter().map(|t| t.as_str()).collect();
        consumer
            .subscribe(&topic_strs)
            .expect("Failed to subscribe");

        Self {
            consumer,
            shutdown: CancellationToken::new(),
            revoke,
        }
    }
}

impl PullSource for KafkaSource {
    fn stream(&self) -> Pin<Box<dyn Stream<Item = StageResult<KafkaPayload>> + '_>> {
        let shutdown = self.shutdown.clone();
        let revoke = &self.revoke;

        Box::pin(stream! {
            let mut kafka_stream = self.consumer.stream();

            loop {
                tokio::select! {
                    msg = kafka_stream.next() => {
                        match msg {
                            Some(Ok(m)) => {
                                yield StageResult::Emit(PipelineEnvelope::from_kafka(&m));
                            }
                            Some(Err(e)) if kafka_poll_error_is_recoverable(&e) => {
                                tracing::warn!("Recoverable Kafka error, skipping: {}", e);
                                continue;
                            }
                            Some(Err(e)) => {
                                tracing::error!("Fatal Kafka error: {}", e);
                                yield StageResult::Fail(Box::new(e));
                                return;
                            }
                            None => {
                                yield StageResult::Exit(PipelineExit::Complete);
                                return;
                            }
                        }
                    }
                    _ = shutdown.cancelled() => {
                        yield StageResult::Exit(PipelineExit::Shutdown);
                        return;
                    }
                    _ = revoke.notified() => {
                        yield StageResult::Exit(PipelineExit::Rebalance);
                        return;
                    }
                }
            }
        })
    }

    fn committer(&self) -> &dyn OffsetCommitter {
        self
    }

    fn shutdown(&self) {
        self.shutdown.cancel();
    }
}

impl OffsetCommitter for KafkaSource {
    fn commit_offsets(
        &self,
        positions: &HashMap<Partition, u64>,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        let mut tpl = TopicPartitionList::new();
        for (partition, offset) in positions {
            tpl.add_partition_offset(
                partition.topic.as_str(),
                partition.index as i32,
                rdkafka::Offset::Offset(*offset as i64),
            )
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send>)?;
        }
        // Async commit matches the existing push model's behavior. The broker
        // may not ack before we clear tracked offsets, but this is acceptable —
        // worst case on crash is re-processing already-committed messages.
        self.consumer
            .commit(&tpl, CommitMode::Async)
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send>)
    }
}
