#!/bin/sh

docker run \
    --name sentry_zookeeper \
    -d --network host \
    -e ZOOKEEPER_CLIENT_PORT=2181 \
    confluentinc/cp-zookeeper:6.2.0

docker run \
    --name sentry_kafka \
    -d --network host \
    -e KAFKA_ZOOKEEPER_CONNECT=127.0.0.1:2181 \
    -e KAFKA_LISTENERS=INTERNAL://0.0.0.0:9093,EXTERNAL://0.0.0.0:9092 \
    -e KAFKA_ADVERTISED_LISTENERS=INTERNAL://127.0.0.1:9093,EXTERNAL://127.0.0.1:9092 \
    -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT \
    -e KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL \
    -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
    confluentinc/cp-kafka:6.2.0
