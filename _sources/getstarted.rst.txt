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

    docker network create arroyo

    docker run --rm \
        -v zookeeper_volume:/var/lib/zookeeper \
        --env ZOOKEEPER_CLIENT_PORT=2181 \
        --name=sentry_zookeeper \
        --network=arroyo \
        -p 2181:2181 \
        confluentinc/cp-zookeeper:6.2.0

    docker run --rm \
        -v kafka_volume:/var/lib/kafka \
        --env KAFKA_ZOOKEEPER_CONNECT=sentry_zookeeper:2181 \
        --env KAFKA_LISTENERS=INTERNAL://0.0.0.0:9093,EXTERNAL://0.0.0.0:9092 \
        --env KAFKA_ADVERTISED_LISTENERS=INTERNAL://127.0.0.1:9093,EXTERNAL://127.0.0.1:9092 \
        --env KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT \
        --env KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL \
        --env KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
        --env CONFLUENT_SUPPORT_METRICS_ENABLE=false \
        --env KAFKA_LOG4J_LOGGERS=kafka.cluster=WARN,kafka.controller=WARN,kafka.coordinator=WARN,kafka.log=WARN,kafka.server=WARN,kafka.zookeeper=WARN,state.change.logger=WARN \
        --env KAFKA_LOG4J_ROOT_LOGLEVEL=WARN \
        --env KAFKA_TOOLS_LOG4J_LOGLEVEL=WARN \
        --name=sentry_kafka \
        --network=arroyo \
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
        --bootstrap-server 127.0.0.1:9092

    docker exec sentry_kafka kafka-topics \
        --create \
        --topic dest-topic \
        --bootstrap-server 127.0.0.1:9092

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
            bootstrap_servers=["127.0.0.1:9092"],
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

    echo "MESSAGE" | kcat -P -b 127.0.0.1:9092 -t source-topic

In a while the message should appear on the console:

.. code-block:: Bash

    MSG: KafkaPayload(key=None, value=b'MESSAGE', headers=[])


Create a streaming consumer
---------------------------

Add a `ProcessingStrategy` and `ProcessingStrategyFactory`.
Here we are using the `RunTask` strategy which runs a custom function over each message.

.. code-block:: Python

    from typing import Mapping

    from arroyo.backends.kafka import KafkaPayload
    from arroyo.processing.strategies import (
        CommitOffsets,
        ProcessingStrategy,
        ProcessingStrategyFactory,
        RunTask,
    )
    from arroyo.types import Commit, Message, Partition, Topic


    def handle_message(message: Message[KafkaPayload]) -> Message[KafkaPayload]:
        print(f"MSG: {message.payload}")
        return message

    class ConsumerStrategyFactory(ProcessingStrategyFactory[KafkaPayload]):
        """
        The factory manages the lifecycle of the `ProcessingStrategy`.
        A strategy is created every time new partitions are assigned to the
        consumer, while it is destroyed when partitions are revoked or the
        consumer is closed
        """
        def create_with_partitions(
            self,
            commit: Commit,
            partitions: Mapping[Partition, int],
        ) -> ProcessingStrategy[KafkaPayload]:
            return RunTask(handle_message, CommitOffsets(commit))

The code above is orchestrated by the Arroyo runtime called `StreamProcessor`.

.. code-block:: Python

    from arroyo.processing import StreamProcessor
    from arroyo.commit import ONCE_PER_SECOND

    processor = StreamProcessor(
        consumer=consumer,
        topic=TOPIC,
        processor_factory=ConsumerStrategyFactory(),
        commit_policy=ONCE_PER_SECOND,
    )

    processor.run()

The main consumer loop is managed by the `StreamProcessor` no need to periodically poll the
consumer. The `ConsumerStrategy` works by inversion of control.

Add some useful logic
---------------------

Now we will chain the `Produce` strategy to produce messages on a second topic after the message is logged

.. code-block:: Python

    from arroyo.backends.kafka import KafkaProducer
    from arroyo.backends.kafka.configuration import build_kafka_configuration
    from arroyo.processing.strategies import Produce

    class ConsumerStrategyFactory(ProcessingStrategyFactory[KafkaPayload]):
        """
        The factory manages the lifecycle of the `ProcessingStrategy`.
        A strategy is created every time new partitions are assigned to the
        consumer, while it is destroyed when partitions are revoked or the
        consumer is closed
        """
        def create_with_partitions(
            self,
            commit: Commit,
            partitions: Mapping[Partition, int],
        ) -> ProcessingStrategy[KafkaPayload]:
            producer = KafkaProducer(
                build_kafka_configuration(
                    default_config={},
                    bootstrap_servers=BOOTSTRAP_SERVERS,
                )
            )

            return RunTask(
                handle_message,
                Produce(producer, Topic("dest-topic"), CommitOffsets(commit))
            )

The message is first passed to the `RunTask` strategy which simply logs the message and submits
the output to the next step. The `Produce` strategy produces the message asynchronously. Once
the message is produced, the `CommitOffsets` strategy commits the offset of the message.

Further examples
================

Find some complete `examples of usage <https://github.com/getsentry/arroyo/tree/main/examples>`_.
