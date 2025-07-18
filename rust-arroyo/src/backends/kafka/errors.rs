use rdkafka::error::{KafkaError, RDKafkaErrorCode};

use crate::backends::ConsumerError;
use crate::backends::ProducerError;

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
        match err {
            KafkaError::MessageProduction(_) => {
                let code = err.rdkafka_error_code().unwrap();
                ProducerError::MessageProductionFailed { code }
            }
            KafkaError::Flush(_) => {
                let code = err.rdkafka_error_code().unwrap();
                ProducerError::FlushFailed { code }
            }
            other => {
                let code = other.rdkafka_error_code().unwrap();
                ProducerError::BrokerError { code }
            }
        }
    }
}
