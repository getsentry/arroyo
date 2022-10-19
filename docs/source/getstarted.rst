===========================
Getting started with Arroyo
===========================

This tutorial shows how to create a Kafka consumer with Arroyo from scratch.

Setup
=====

This section explains how to setup Kafka, Zookeeper and install the library

Kafka and Zookeeper
-------------------

In order to run an arroyo Kafka consumer you will need a working Kafka broker.
If you already have one, you can skip this step.
If you do not have a running Kafka broker, this command will install and start
a Kafka docker container. (It requires Docker to be installed).

.. code-block:: Bash

    docker run   --rm \
        -v zookeeper_volume:/var/lib/zookeeper \
        --env ZOOKEEPER_CLIENT_PORT=2181 \
        --name=zookeeper \
        -p 2181:2181 \
        confluentinc/cp-zookeeper:6.2.0

    docker run   --rm \
        -v kafka_volume:/var/lib/kafka \
        --env KAFKA_ZOOKEEPER_CONNECT=localhost:2181 \
        --env KAFKA_LISTENERS=INTERNAL://0.0.0.0:9093,EXTERNAL://0.0.0.0:9092 \
        --env KAFKA_ADVERTISED_LISTENERS=INTERNAL://localhost:9093,EXTERNAL://localhost:9092 \
        --env KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT \
        --env KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL \
        --env KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
        --env CONFLUENT_SUPPORT_METRICS_ENABLE=false \
        --env KAFKA_LOG4J_LOGGERS=kafka.cluster=WARN,kafka.controller=WARN,kafka.coordinator=WARN,kafka.log=WARN,kafka.server=WARN,kafka.zookeeper=WARN,state.change.logger=WARN \
        --env KAFKA_LOG4J_ROOT_LOGLEVEL=WARN \
        --env KAFKA_TOOLS_LOG4J_LOGLEVEL=WARN \
        --name=kafka \
        -p 9092:9092 \
        confluentinc/cp-kafka:6.2.0

Now you should see Kafka and Zookeeper running with

.. code-block:: Bash

    docker ps

Install Kafkacat
----------------

This tool will be useful to produce onto and consume from topics.

.. code-block:: Bash

    https://docs.confluent.io/platform/current/app-development/kafkacat-usage.html#kcat-formerly-kafkacat-utility


Development environment
-----------------------

You will need to install the library. Most likely in a python venv. So first, create a python virtual
environment. Then you can install arroyo with this.

.. code-block:: Bash

   pip install sentry-arroyo

Create two Kafka topics
-----------------------

Our example will consume from one topic and produce the same messages on another topic. So we need
two topics.

.. code-block:: Bash

    docker exec sentry_kafka kafka-topics \
        --create \
        --topic source-topic \
        --bootstrap-server localhost:9092

    docker exec sentry_kafka kafka-topics \
        --create \
        --topic dest-topic \
        --bootstrap-server localhost:9092

Now you should be ready to develop with Arroyo.

Create a basic consumer
=======================

Arroyo provides two level of abstractions when writing a consumer: the basic consumer/producer library
and the Streaming library. The first is just a thin wrapper around a librdkafka consumer/producer that
adds some features around offset management. The second provides a more abstract streaming interface
that hides details like rebalancing and the consumer lifecycle.

Creating a basic consumer
-------------------------

This initializes a basic consumer and consumes a message.

.. code-block:: Python

    from arroyo.backends.kafka.configuration import (
        build_kafka_consumer_configuration,
    )
    from arroyo.backends.kafka.consumer import KafkaConsumer
    from arroyo.types import Topic

    TOPIC = Topic("source-topic")

    consumer = KafkaConsumer(
        build_kafka_consumer_configuration(
            default_config={},
            bootstrap_servers=["localhost:9092"],
            auto_offset_reset="latest",
            group_id="test-group",
        )
    )

    consumer.subscribe([TOPIC])

    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is not None:
            print(f"MSG: {msg.payload}")

Start this script and use kcat to produce a message:

.. code-block:: Bash

    echo "MESSAGE" | kcat -P -b localhost:9092 -t source-topic

In a while the message should appear on the console:

.. code-block:: Bash

    MSG: KafkaPayload(key=None, value=b'MESSAGE', headers=[])


Create a streaming consumer
---------------------------

Add a `ProcessingStragey` and `ProcessingStrategyFactory`.

.. code-block:: Python

    class ConsumerStrategy(ProcessingStrategy[KafkaPayload]):
        """
        The strategy implements the streaming interface.
        The runtime submits work to the strategy via the `submit`
        method. Which is supposed to not be blocking.
        Periodically the runtime invokes `poll` which is where the
        work is supposed to be done.
        """

        def __init__(
            self,
            committer: Callable[[Mapping[Partition, Position]], None],
            partitions: Mapping[Partition, int],
        ):
            print(f"Partitions assigned {partitions}")

        def poll(self) -> None:
            pass

        def submit(self, message: Message[KafkaPayload]) -> None:
            # Receives work to do
            print(f"MSG: {message.payload}")

        def close(self) -> None:
            pass

        def terminate(self) -> None:
            print("Terminating")

        def join(self, timeout: Optional[float] = None) -> None:
            pass


    class ConsumerStrategyFactory(ProcessingStrategyFactory[KafkaPayload]):
        """
        The factory manages the lifecycle of the `ProcessingStrategy`.
        A strategy is created every time new partitions are assigned to the
        consumer, while it is destroyed when partitions are revoked or the
        consumer is closed
        """

        def __init__(self):
            pass

        def create_with_partitions(
            self,
            commit: Callable[[Mapping[Partition, Position]], None],
            partitions: Mapping[Partition, int],
        ) -> ProcessingStrategy[KafkaPayload]:
            return ConsumerStrategy(commit, partitions)

The code above is orchestrated by the Arroyo runtime called `StreamingProcessor`.

.. code-block:: Python

    processor = StreamProcessor(
        consumer=consumer,
        topic=TOPIC,
        processor_factory=ConsumerStrategyFactory(),
    )

    processor.run()

The main consumer loop is managed by the `StreamProcessor` no need to periodically poll the
consumer. The `StreamingStrategy` works by inversion of control.

Add some useful logic
---------------------

Now we will add some logic to the `ProcessingStrategy` to produce messages on a second topic.

.. code-block:: Python

    class ConsumerStrategy(ProcessingStrategy[KafkaPayload]):
        def __init__(
            self,
            committer: Callable[[Mapping[Partition, Position]], None],
            partitions: Mapping[Partition, int],
        ):
            print(f"Partitions assigned {partitions}")
            self.__callbacks = []
            self.__producer = KafkaProducer(
                build_kafka_configuration(
                    default_config={},
                    bootstrap_servers=BOOTSTRAP_SERVERS,
                )
            )

        def poll(self) -> None:
            while self.__callbacks and self.__callbacks[0].future.done():
                self.__callbacks.popleft()

        def submit(self, message: Message[KafkaPayload]) -> None:
            # do something with the message
            # The produce operation is asynchronous
            callback = self.__producer.produce(
                destination=Topic("dest-topic"),
                payload=message.payload
            )
            self.__callbacks.append(callback)

        def close(self) -> None:
            self.__producer.close()

        def terminate(self) -> None:
            print("Terminating")

        def join(self, timeout: Optional[float] = None) -> None:
            while self.__callbacks
                c = self.__callbacks.popleft()
                c.result()

This code asynchronously produces all messages received. When `submit` is invoked, the message is
produced asynchronously. The method is not blocking.
The `produce` returns a callback as a future. If we wanted to do something with the result we would
do it in the poll on the completed callback. When the consumer is stopped, or the partitions are
revoked, we wait for all the missing callbacks to complete in the `join` method.


Further examples
================

Find some complete `examples of usage <https://github.com/getsentry/arroyo/tree/main/examples>`_.
