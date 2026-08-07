//! Rebalance synchronization for the pull-based pipeline.
//!
//! Bridges the sync rebalance callback (rdkafka's thread) with the
//! async pipeline drain (tokio runtime). See `RebalanceSync` for
//! the full flow.

use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext};
use rdkafka::types::RDKafkaRespErr;
use rdkafka::{ClientContext, TopicPartitionList};
use tokio::sync::Notify;

/// Synchronizes the rebalance callback (sync, rdkafka thread) with the
/// pipeline drain (async, tokio runtime).
///
/// Flow:
///   1. rdkafka calls rebalance() → signals `revoke` Notify, waits on condvar
///   2. Pipeline's select! picks up the Notify → yields Exit(Rebalance)
///   3. Pipeline drains, commit() flushes offsets
///   4. PipelineRunner calls signal_drain_complete() → condvar wakes
///   5. rebalance() callback unblocks → calls base_consumer.unassign()
pub(crate) struct RebalanceSync {
    pub(crate) revoke: Notify,
    drain_complete: (Mutex<bool>, Condvar),
}

const DRAIN_TIMEOUT: Duration = Duration::from_secs(10);

impl RebalanceSync {
    pub(crate) fn new() -> Self {
        Self {
            revoke: Notify::new(),
            drain_complete: (Mutex::new(false), Condvar::new()),
        }
    }

    /// Block until the pipeline signals drain is complete, or timeout.
    fn wait_for_drain(&self) {
        let (lock, cvar) = &self.drain_complete;
        let mut drained = lock.lock().unwrap();
        if !*drained {
            let (guard, result) = cvar.wait_timeout(drained, DRAIN_TIMEOUT).unwrap();
            drained = guard;
            if result.timed_out() {
                tracing::warn!(
                    "Rebalance drain timed out after {}s, some offsets may be lost",
                    DRAIN_TIMEOUT.as_secs()
                );
            }
        }
        *drained = false; // reset for next rebalance
    }

    /// Signal that drain is complete (called from async side).
    pub(crate) fn signal_complete(&self) {
        let (lock, cvar) = &self.drain_complete;
        let mut drained = lock.lock().unwrap();
        *drained = true;
        cvar.notify_one();
    }
}

/// ConsumerContext that blocks on partition revocation until the pipeline
/// has drained and committed offsets. Matches the push model's pattern
/// of flushing before unassign.
pub(crate) struct PullRebalanceContext {
    pub(crate) sync: Arc<RebalanceSync>,
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
            tracing::info!("Partition revocation detected, draining pipeline");

            // Signal the pipeline to stop
            self.sync.revoke.notify_one();

            // Wait for the pipeline to drain and commit offsets
            self.sync.wait_for_drain();

            // Safe to unassign now — offsets are committed
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
