use rdkafka::error::{KafkaError, RDKafkaErrorCode};

use crate::backends::ConsumerError;
use crate::backends::ProducerError;

/// Returns a string representation of the KafkaError variant name and any embedded RDKafkaErrorCode
pub fn get_error_name(error: &KafkaError) -> String {
    match error {
        // Variants with RDKafkaErrorCode - include the error code
        KafkaError::AdminOp(code) => format!("AdminOp({})", code),
        KafkaError::ConsumerCommit(code) => format!("ConsumerCommit({})", code),
        KafkaError::ConsumerQueueClose(code) => format!("ConsumerQueueClose({})", code),
        KafkaError::Flush(code) => format!("Flush({})", code),
        KafkaError::Global(code) => format!("Global({})", code),
        KafkaError::GroupListFetch(code) => format!("GroupListFetch({})", code),
        KafkaError::MessageConsumption(code) => format!("MessageConsumption({})", code),
        KafkaError::MessageConsumptionFatal(code) => format!("MessageConsumptionFatal({})", code),
        KafkaError::MessageProduction(code) => format!("MessageProduction({})", code),
        KafkaError::MetadataFetch(code) => format!("MetadataFetch({})", code),
        KafkaError::OffsetFetch(code) => format!("OffsetFetch({})", code),
        KafkaError::SetPartitionOffset(code) => format!("SetPartitionOffset({})", code),
        KafkaError::StoreOffset(code) => format!("StoreOffset({})", code),
        KafkaError::Transaction(code) => format!("Transaction({})", code),
        KafkaError::MockCluster(code) => format!("MockCluster({})", code),

        // Variants without RDKafkaErrorCode - just return variant name
        KafkaError::AdminOpCreation(_) => "AdminOpCreation".to_string(),
        KafkaError::Canceled => "Canceled".to_string(),
        KafkaError::ClientConfig(_, _, _, _) => "ClientConfig".to_string(),
        KafkaError::ClientCreation(_) => "ClientCreation".to_string(),
        KafkaError::NoMessageReceived => "NoMessageReceived".to_string(),
        KafkaError::Nul(_) => "Nul".to_string(),
        KafkaError::PartitionEOF(_) => "PartitionEOF".to_string(),
        KafkaError::PauseResume(_) => "PauseResume".to_string(),
        KafkaError::Rebalance(_) => "Rebalance".to_string(),
        KafkaError::Seek(_) => "Seek".to_string(),
        KafkaError::Subscription(_) => "Subscription".to_string(),

        #[allow(unreachable_patterns)]
        _ => "Unknown".to_string(),
    }
}

impl From<KafkaError> for ConsumerError {
    fn from(err: KafkaError) -> Self {
        match err {
            KafkaError::OffsetFetch(RDKafkaErrorCode::OffsetOutOfRange) => {
                ConsumerError::OffsetOutOfRange {
                    source: Box::new(err),
                }
            }
            other => ConsumerError::BrokerError(Box::new(other)),
        }
    }
}

impl From<KafkaError> for ProducerError {
    fn from(err: KafkaError) -> Self {
        ProducerError::ProducerFailure {
            error: get_error_name(&err),
        }
    }
}
