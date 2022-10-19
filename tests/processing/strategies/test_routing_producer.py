from datetime import datetime
from typing import MutableSequence, Optional
from unittest import mock

from confluent_kafka import Producer

from arroyo.backends.kafka import KafkaPayload
from arroyo.backends.local.backend import LocalBroker
from arroyo.backends.local.storages.memory import MemoryMessageStorage
from arroyo.processing.strategies.routing_producer import (
    MessageRoute,
    MessageRouter,
    RoutingProducerStep,
)
from arroyo.types import Message, Partition, Topic
from arroyo.utils.clock import TestingClock


class RoundRobinRouter(MessageRouter):
    def __init__(self) -> None:
        self.all_broker_storages: MutableSequence[
            MemoryMessageStorage[KafkaPayload]
        ] = []
        self.all_producers: MutableSequence[Producer] = []
        clock = TestingClock()

        for i in range(3):
            broker_storage: MemoryMessageStorage[KafkaPayload] = MemoryMessageStorage()
            broker: LocalBroker[KafkaPayload] = LocalBroker(broker_storage, clock)
            broker.create_topic(Topic(f"result-topic-{i}"), partitions=1)
            self.all_broker_storages.append(broker_storage)
            self.all_producers.append(broker.get_producer())

    def get_route_for_message(self, message: Message[KafkaPayload]) -> MessageRoute:
        routing_key, routing_value = message.payload.headers[0]
        dest_id = int(routing_value) % len(self.all_producers)
        return MessageRoute(
            self.all_producers[dest_id], Topic(f"result-topic-{dest_id}")
        )

    def shutdown(self, timeout: Optional[float] = None) -> None:
        for producer in self.all_producers:
            producer.flush()


def test_routing_producer() -> None:
    """
    Test that the routing producer step correctly routes messages to the desired
    producer and topic. This uses the RoundRobinRouter, which routes messages to
    three different producers and topics
    """
    epoch = datetime(1970, 1, 1)
    orig_topic = Topic("orig-topic")

    commit = mock.Mock()

    router = RoundRobinRouter()
    strategy = RoutingProducerStep(
        commit_function=commit,
        message_router=router,
    )

    for i in range(3):
        value = b'{"something": "something"}'
        data = KafkaPayload(None, value, [("key", b"%d" % i)])
        message = Message(
            Partition(orig_topic, 0),
            1,
            data,
            epoch,
        )

        strategy.submit(message)

        # Consume message from the broker and result topic on which we expect
        # the message to be routed to
        produced_message = router.all_broker_storages[i].consume(
            Partition(Topic(f"result-topic-{i}"), 0), 0
        )
        assert produced_message is not None
        assert produced_message.payload.value == value
        assert (
            router.all_broker_storages[i].consume(
                Partition(Topic(f"result-topic-{i}"), 0), 1
            )
            is None
        )

        strategy.poll()
        assert commit.call_count == i + 1
