# Consume -> Transform -> Produce

### What this does

---

This is an example of using a `KafkaConsumer`, `KafkaProducer`, and the `StreamProcessor` together.

The process is fairly simple:

- The consumer consumes messages from the `raw-topic`
- This message is in the form `{"username": "<username>", "password": "<password>"}`
- The message is submitted to the `TransformStrategy` which hashes the password string
- The credentials are then submitted to `Produce` which simply produces the given message to `hash-topic`
- Finally the `CommitOffsets` step commits the original offset back for the consumer

### Usage

---

To run this example ensure each of your shells are inside the examples/transfrom_and_produce directory. To begin, build the session by running `docker-compose up`. Once Kafka has entered a running state you may continue onto the next section.

Note you will see `transform_and_produce-arroyo-1 exited with code 0`. You can safely ignore it. This is expected behavior.

**Monitoring your raw-topic consumer.**

This will echo every message produced to the `raw-topic` topic.

```shell
docker-compose exec kafka kafka-console-consumer --bootstrap-server kafka:9095 --topic raw-topic
```

**Running your raw-topic producer.**

This process produces the messages that will flow through the system. We will need to return to this shell to enter commands once we've finished setup.

```shell
docker-compose exec kafka kafka-console-producer --bootstrap-server kafka:9095 --topic raw-topic
```

**Monitoring your hashed-topic consumer.**

This will echo every message produced to the `hashed-topic` topic. This is your post-transformation output consumer. You should see the transformations performed by Arroyo here.

```shell
docker-compose exec kafka kafka-console-consumer --bootstrap-server kafka:9095 --topic hashed-topic
```

**Running your arroyo middleware.**

```shell
docker-compose run arroyo python /app/script.py
```

**Seeing it all come together.**

Produce a message by returning to our producer shell. Provide the producer a message formatted similarly to this one: `{"username": "user1", "password": "Password1!"}`.

Now the message should appear in the `raw-topic` shell, followed by another message in the `hashed-topic` shell. The password field of the message in `hashed-topic` should be the SHA256 hash of the password field of the message we manually produced to `raw-topic`.
