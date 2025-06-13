use std::any::type_name;
use std::cmp::Eq;
use std::collections::{BTreeMap, HashSet};
use std::fmt;
use std::hash::Hash;
use std::sync::Mutex;

use chrono::{DateTime, Utc};
use once_cell::sync::Lazy;

#[derive(Clone, Copy, Eq, Hash, PartialEq, PartialOrd, Ord)]
pub struct Topic(&'static str);

impl Topic {
    pub fn new(name: &str) -> Self {
        static INTERNED_TOPICS: Lazy<Mutex<HashSet<String>>> = Lazy::new(Default::default);
        let mut interner = INTERNED_TOPICS.lock().unwrap();
        interner.insert(name.into());
        let interned_name = interner.get(name).unwrap();

        // SAFETY:
        // - The interner is static, append-only, and only defined within this function.
        // - We insert heap-allocated `String`s that do not move.
        let interned_name = unsafe { std::mem::transmute::<&str, &'static str>(interned_name) };
        Self(interned_name)
    }

    pub fn as_str(&self) -> &str {
        self.0
    }
}

impl fmt::Debug for Topic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = self.as_str();
        f.debug_tuple("Topic").field(&s).finish()
    }
}

impl fmt::Display for Topic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, PartialOrd, Ord)]
pub struct Partition {
    pub topic: Topic,
    pub index: u16,
}

impl Partition {
    pub fn new(topic: Topic, index: u16) -> Self {
        Self { topic, index }
    }
}

impl fmt::Display for Partition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Partition({} topic={})", self.index, &self.topic)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TopicOrPartition {
    Topic(Topic),
    Partition(Partition),
}

impl TopicOrPartition {
    pub fn topic(&self) -> Topic {
        match self {
            TopicOrPartition::Topic(topic) => *topic,
            TopicOrPartition::Partition(partition) => partition.topic,
        }
    }
}

impl From<Topic> for TopicOrPartition {
    fn from(value: Topic) -> Self {
        Self::Topic(value)
    }
}

impl From<Partition> for TopicOrPartition {
    fn from(value: Partition) -> Self {
        Self::Partition(value)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct BrokerMessage<T> {
    pub payload: T,
    pub partition: Partition,
    pub offset: u64,
    pub timestamp: DateTime<Utc>,
}

impl<T> BrokerMessage<T> {
    pub fn new(payload: T, partition: Partition, offset: u64, timestamp: DateTime<Utc>) -> Self {
        Self {
            payload,
            partition,
            offset,
            timestamp,
        }
    }

    pub fn replace<TReplaced>(self, replacement: TReplaced) -> BrokerMessage<TReplaced> {
        BrokerMessage {
            payload: replacement,
            partition: self.partition,
            offset: self.offset,
            timestamp: self.timestamp,
        }
    }

    /// Map a fallible function over this messages's payload.
    pub fn try_map<TReplaced, E, F: FnOnce(T) -> Result<TReplaced, E>>(
        self,
        f: F,
    ) -> Result<BrokerMessage<TReplaced>, E> {
        let Self {
            payload,
            partition,
            offset,
            timestamp,
        } = self;

        let payload = f(payload)?;

        Ok(BrokerMessage {
            payload,
            partition,
            offset,
            timestamp,
        })
    }
}

impl<T> fmt::Display for BrokerMessage<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "BrokerMessage(partition={} offset={})",
            self.partition, self.offset
        )
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct AnyMessage<T> {
    pub payload: T,
    pub committable: BTreeMap<Partition, u64>,
}

impl<T> AnyMessage<T> {
    pub fn new(payload: T, committable: BTreeMap<Partition, u64>) -> Self {
        Self {
            payload,
            committable,
        }
    }

    pub fn replace<TReplaced>(self, replacement: TReplaced) -> AnyMessage<TReplaced> {
        AnyMessage {
            payload: replacement,
            committable: self.committable,
        }
    }

    /// Map a fallible function over this messages's payload.
    pub fn try_map<TReplaced, E, F: FnOnce(T) -> Result<TReplaced, E>>(
        self,
        f: F,
    ) -> Result<AnyMessage<TReplaced>, E> {
        let Self {
            payload,
            committable,
        } = self;

        let payload = f(payload)?;

        Ok(AnyMessage {
            payload,
            committable,
        })
    }
}

impl<T> fmt::Display for AnyMessage<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "AnyMessage(committable={:?})", self.committable)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum InnerMessage<T> {
    BrokerMessage(BrokerMessage<T>),
    AnyMessage(AnyMessage<T>),
}

#[derive(Clone, Debug, PartialEq)]
pub struct Message<T> {
    pub inner_message: InnerMessage<T>,
}

impl<T> Message<T> {
    pub fn new_broker_message(
        payload: T,
        partition: Partition,
        offset: u64,
        timestamp: DateTime<Utc>,
    ) -> Self {
        Self {
            inner_message: InnerMessage::BrokerMessage(BrokerMessage {
                payload,
                partition,
                offset,
                timestamp,
            }),
        }
    }

    pub fn new_any_message(payload: T, committable: BTreeMap<Partition, u64>) -> Self {
        Self {
            inner_message: InnerMessage::AnyMessage(AnyMessage {
                payload,
                committable,
            }),
        }
    }

    pub fn payload(&self) -> &T {
        match &self.inner_message {
            InnerMessage::BrokerMessage(BrokerMessage { payload, .. }) => payload,
            InnerMessage::AnyMessage(AnyMessage { payload, .. }) => payload,
        }
    }

    /// Consumes the message and returns its payload.
    pub fn into_payload(self) -> T {
        match self.inner_message {
            InnerMessage::BrokerMessage(BrokerMessage { payload, .. }) => payload,
            InnerMessage::AnyMessage(AnyMessage { payload, .. }) => payload,
        }
    }

    /// Consumes the message, returns an empty message with committables and the payload without
    /// copying
    /// # Examples:
    /// ```
    /// use std::collections::BTreeMap;
    /// use sentry_arroyo::types::Message;
    /// use sentry_arroyo::types::{Partition, Topic};
    ///
    /// // Create the message with the committable structure we want to preserve
    /// let topic = Topic::new("test");
    /// let part = Partition { topic, index: 10 };
    /// let committable: BTreeMap<Partition, u64> = vec![
    ///        (part, 42069)
    /// ].into_iter().collect();
    /// let message = Message::new_any_message("my_payload".to_string(), committable);
    ///
    /// fn transform_msg(msg: Message<String>) -> Message<usize> {
    ///     // transform_msg takes ownership of the Message object
    ///     let (empty_message_with_commitable, payload) = msg.take();
    ///     empty_message_with_commitable.replace(payload.len())
    /// }
    ///
    /// let transformed_msg = transform_msg(message);
    /// ```
    pub fn take(self) -> (Message<()>, T) {
        match self.inner_message {
            InnerMessage::BrokerMessage(bm) => (
                Message::new_broker_message((), bm.partition, bm.offset, bm.timestamp), bm.payload),
            InnerMessage::AnyMessage(am) => (
                Message::new_any_message((), am.committable),
                am.payload,
            )
        }
    }


    /// Returns an iterator over this message's committable offsets.
    pub fn committable(&self) -> Committable {
        match &self.inner_message {
            InnerMessage::BrokerMessage(BrokerMessage {
                partition, offset, ..
            }) => Committable(CommittableInner::Broker(std::iter::once((
                *partition,
                offset + 1,
            )))),
            InnerMessage::AnyMessage(AnyMessage { committable, .. }) => {
                Committable(CommittableInner::Any(committable.iter()))
            }
        }
    }

    pub fn replace<TReplaced>(self, replacement: TReplaced) -> Message<TReplaced> {
        match self.inner_message {
            InnerMessage::BrokerMessage(inner) => Message {
                inner_message: InnerMessage::BrokerMessage(inner.replace(replacement)),
            },
            InnerMessage::AnyMessage(inner) => Message {
                inner_message: InnerMessage::AnyMessage(inner.replace(replacement)),
            },
        }
    }

    /// Map a fallible function over this messages's payload.
    pub fn try_map<TReplaced, E, F: Fn(T) -> Result<TReplaced, E>>(
        self,
        f: F,
    ) -> Result<Message<TReplaced>, E> {
        match self.inner_message {
            InnerMessage::BrokerMessage(inner) => {
                let inner = inner.try_map(f)?;
                Ok(inner.into())
            }
            InnerMessage::AnyMessage(inner) => {
                let inner = inner.try_map(f)?;
                Ok(inner.into())
            }
        }
    }

    // Returns this message's timestamp, if it has one.
    pub fn timestamp(&self) -> Option<DateTime<Utc>> {
        match &self.inner_message {
            InnerMessage::BrokerMessage(m) => Some(m.timestamp),
            InnerMessage::AnyMessage(_) => None,
        }
    }
}

impl<T> fmt::Display for Message<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.inner_message {
            InnerMessage::BrokerMessage(BrokerMessage {
                partition, offset, ..
            }) => {
                write!(
                    f,
                    "Message<{}>(partition={partition}), offset={offset}",
                    type_name::<T>(),
                )
            }
            InnerMessage::AnyMessage(AnyMessage { committable, .. }) => {
                write!(
                    f,
                    "Message<{}>(committable={committable:?})",
                    type_name::<T>(),
                )
            }
        }
    }
}

impl<T> From<BrokerMessage<T>> for Message<T> {
    fn from(value: BrokerMessage<T>) -> Self {
        Self {
            inner_message: InnerMessage::BrokerMessage(value),
        }
    }
}

impl<T> From<AnyMessage<T>> for Message<T> {
    fn from(value: AnyMessage<T>) -> Self {
        Self {
            inner_message: InnerMessage::AnyMessage(value),
        }
    }
}

#[derive(Debug, Clone)]
enum CommittableInner<'a> {
    Any(std::collections::btree_map::Iter<'a, Partition, u64>),
    Broker(std::iter::Once<(Partition, u64)>),
}

/// An iterator over a `Message`'s committable offsets.
///
/// This is produced by [`Message::committable`].
#[derive(Debug, Clone)]
pub struct Committable<'a>(CommittableInner<'a>);

impl Iterator for Committable<'_> {
    type Item = (Partition, u64);

    fn next(&mut self) -> Option<Self::Item> {
        match self.0 {
            CommittableInner::Any(ref mut inner) => inner.next().map(|(k, v)| (*k, *v)),
            CommittableInner::Broker(ref mut inner) => inner.next(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use super::{BrokerMessage, Message, Partition, Topic, InnerMessage};
    use chrono::Utc;

    #[test]
    fn message() {
        let now = Utc::now();
        let topic = Topic::new("test");
        let part = Partition { topic, index: 10 };
        let message = BrokerMessage::new("payload".to_string(), part, 10, now);

        assert_eq!(message.partition.topic.as_str(), "test");
        assert_eq!(message.partition.index, 10);
        assert_eq!(message.offset, 10);
        assert_eq!(message.payload, "payload");
        assert_eq!(message.timestamp, now);
    }


    #[test]
    fn broker_message_take() {
        let now = Utc::now();
        let topic = Topic::new("test");
        let part = Partition { topic, index: 10 };
        let committable: BTreeMap<Partition, u64> = vec![
            (part, 42069)
        ].into_iter().collect();



        let b_message = Message::new_broker_message("payload".to_string(), part, 10, now);
        let a_message = Message::new_any_message("payload".to_string(), committable.clone());

        // need something to take ownership of the message
        let transform_func = |bm: Message<String>| -> Message<usize> {
            let (empty_message, payload) = bm.take();
            return empty_message.replace(payload.len());
        };
        let validate_msg = |msg: Message<usize>| -> () {
            match msg.inner_message {
                InnerMessage::BrokerMessage(bm) => {
                    assert_eq!(bm.offset, 10);
                    assert_eq!(bm.partition, part);
                },
                InnerMessage::AnyMessage(am) => {
                    assert_eq!(am.committable, committable);
                }
            }
        };

        let transformed_broker_msg= transform_func(b_message);
        let transformed_any_msg= transform_func(a_message);
        assert_eq!(transformed_any_msg.payload().clone(), "payload".len());
        assert_eq!(transformed_broker_msg.payload().clone(), "payload".len());


        validate_msg(transformed_broker_msg);
        validate_msg(transformed_any_msg);

    }


    #[test]
    fn fmt_display() {
        let now = Utc::now();
        let part = Partition {
            topic: Topic::new("test"),
            index: 10,
        };
        let message = BrokerMessage::new("payload".to_string(), part, 10, now);

        assert_eq!(
            message.to_string(),
            "BrokerMessage(partition=Partition(10 topic=test) offset=10)"
        )
    }
}
