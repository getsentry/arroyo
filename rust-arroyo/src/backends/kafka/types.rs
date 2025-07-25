use crate::types::TopicOrPartition;
use rdkafka::message::Headers as _;
use rdkafka::message::{BorrowedHeaders, Header, OwnedHeaders};
use rdkafka::producer::{BaseRecord, FutureRecord};

use std::sync::Arc;
#[derive(Clone, Debug)]
pub struct Headers {
    headers: OwnedHeaders,
}

impl Headers {
    pub fn new() -> Self {
        Self {
            headers: OwnedHeaders::new(),
        }
    }

    pub fn insert(self, key: &str, value: Option<Vec<u8>>) -> Headers {
        let headers = self.headers.insert(Header {
            key,
            value: value.as_ref(),
        });
        Self { headers }
    }

    pub fn get(&self, key: &str) -> Option<&[u8]> {
        self.headers
            .iter()
            .find(|header| header.key == key)
            .and_then(|header| header.value)
    }
}

impl Default for Headers {
    fn default() -> Self {
        Self::new()
    }
}

impl From<&BorrowedHeaders> for Headers {
    fn from(value: &BorrowedHeaders) -> Self {
        Self {
            headers: value.detach(),
        }
    }
}

impl From<Headers> for OwnedHeaders {
    fn from(value: Headers) -> Self {
        value.headers
    }
}

#[derive(Clone, Debug)]
struct KafkaPayloadInner {
    pub key: Option<Vec<u8>>,
    pub headers: Option<Headers>,
    pub payload: Option<Vec<u8>>,
}

#[derive(Clone, Debug)]
pub struct KafkaPayload {
    inner: Arc<KafkaPayloadInner>,
}

impl<'a> KafkaPayload {
    pub fn new(key: Option<Vec<u8>>, headers: Option<Headers>, payload: Option<Vec<u8>>) -> Self {
        Self {
            inner: Arc::new(KafkaPayloadInner {
                key,
                headers,
                payload,
            }),
        }
    }

    pub fn key(&self) -> Option<&Vec<u8>> {
        self.inner.key.as_ref()
    }

    pub fn headers(&self) -> Option<&Headers> {
        self.inner.headers.as_ref()
    }

    pub fn payload(&self) -> Option<&Vec<u8>> {
        self.inner.payload.as_ref()
    }

    pub fn to_base_record(
        &'a self,
        destination: &'a TopicOrPartition,
    ) -> BaseRecord<'a, Vec<u8>, Vec<u8>> {
        let topic = match destination {
            TopicOrPartition::Topic(topic) => topic.as_str(),
            TopicOrPartition::Partition(partition) => partition.topic.as_str(),
        };

        let partition = match destination {
            TopicOrPartition::Topic(_) => None,
            TopicOrPartition::Partition(partition) => Some(partition.index),
        };

        let mut base_record = BaseRecord::to(topic);

        if let Some(msg_key) = self.key() {
            base_record = base_record.key(msg_key);
        }

        if let Some(msg_payload) = self.payload() {
            base_record = base_record.payload(msg_payload);
        }

        if let Some(headers) = self.headers() {
            base_record = base_record.headers((*headers).clone().into());
        }

        if let Some(index) = partition {
            base_record = base_record.partition(index as i32)
        }

        base_record
    }

    pub fn to_future_record(
        &'a self,
        destination: &'a TopicOrPartition,
    ) -> FutureRecord<'a, Vec<u8>, Vec<u8>> {
        let topic = match destination {
            TopicOrPartition::Topic(topic) => topic.as_str(),
            TopicOrPartition::Partition(partition) => partition.topic.as_str(),
        };

        let partition = match destination {
            TopicOrPartition::Topic(_) => None,
            TopicOrPartition::Partition(partition) => Some(partition.index),
        };

        let mut future_record = FutureRecord::to(topic);

        if let Some(msg_key) = self.key() {
            future_record = future_record.key(msg_key);
        }

        if let Some(msg_payload) = self.payload() {
            future_record = future_record.payload(msg_payload);
        }

        if let Some(headers) = self.headers() {
            future_record = future_record.headers((*headers).clone().into());
        }

        if let Some(index) = partition {
            future_record = future_record.partition(index as i32)
        }

        future_record
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Topic;

    #[test]
    fn test_kafka_payload() {
        let destination = TopicOrPartition::Topic(Topic::new("test"));
        let p: KafkaPayload = KafkaPayload::new(None, None, None);
        let base_record = p.to_base_record(&destination);
        assert_eq!(base_record.topic, "test");
        assert_eq!(base_record.key, None);
        assert_eq!(base_record.payload, None);
        assert_eq!(base_record.partition, None);

        let mut headers = Headers::new();
        headers = headers.insert("version", Some(b"1".to_vec()));
        let p2 = KafkaPayload::new(
            Some(b"key".to_vec()),
            Some(headers),
            Some(b"message".to_vec()),
        );

        let base_record = p2.to_base_record(&destination);
        assert_eq!(base_record.topic, "test");
        assert_eq!(base_record.key, Some(&b"key".to_vec()));
        assert_eq!(base_record.payload, Some(&b"message".to_vec()));
    }

    #[test]
    fn test_headers() {
        let mut headers = self::Headers::new();
        headers = headers.insert("key1", Some(b"value1".to_vec()));
        headers = headers.insert("key2", Some(b"value2".to_vec()));
        headers = headers.insert("key3", Some(b"value3".to_vec()));

        assert_eq!(headers.get("key1"), Some(b"value1").map(|v| v.as_ref()));
        assert_eq!(headers.get("key2"), Some(b"value2").map(|v| v.as_ref()));
        assert_eq!(headers.get("key10"), None);
    }
}
