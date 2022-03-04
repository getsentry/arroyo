# Consume -> Transform -> Produce

### What this does

---

This is an example of using a `KafkaConsumer`, `KafkaProducer`, and the `StreamProcessor` together.

The process is fairly simple:

- The consumer consumes messages from the `raw-topic`
- This message is in the form `{"username": "<username>", "password": "<password>"}`
- The message is submitted to the `HashPasswordStrategy` which hashes the password string
- The credentials are then submitted to the `ProduceStep` which simply produces the given message to `hash-topic`
- The `ProduceStep` is also responsible for commiting the original offset back for the consumer

### Usage

---

Install `kcat` and read up on usage [here](https://docs.confluent.io/platform/current/app-development/kafkacat-usage.html)

<strong>To begin, start the following commands in different shells:</strong>

_Monitor messages produced to `raw-topic`:_

```shell
$ kcat -b localhost:9092 -t raw-topic -X broker.address.family=v4 -C -f '\nValue (%S bytes): %s\nTimestamp: %T\tPartition: %p\tOffset: %o\n--\n'
```

_Monitor messages produced to `hashed-topic`:_

```shell
$ kcat -b localhost:9092 -t hashed-topic -X broker.address.family=v4 -C -f '\nValue (%S bytes): %s\nTimestamp: %T\tPartition: %p\tOffset: %o\n--\n'
```

_Start the script itself:_

```shell
$ python3 arroyo/examples/transform_and_produce/script.py
```

---

<strong>At this point we have all the pieces in place and can start manually producing messages to `raw-topic`:</strong>

Produce a message to the topic by typing in a message in the expected format and hitting `Ctrl-D`.

```shell
$ kcat -b localhost:9092 -t raw-topic -X broker.address.family=v4 -P
{"username": "user1", "password": "Password1!"}
<Ctrl-D>
```

Now the message should appear in the `raw-topic` shell, followed by another message in the `hashed-topic` shell. The password field of the message in `hashed-topic` should be the SHA256 hash of the password field of the message we manually produced to `raw-topic`.
