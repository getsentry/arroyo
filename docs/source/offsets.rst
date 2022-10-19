==================
Committing offsets
==================

Arroyo does not auto commit offsets. It is up to you to manually commit offsets when processing for that
message is completed.

The commit callback will be passed to processing strategy via `ProcessingStrategyFactory.create_with_partitions`.
You should pass this to the strategy and have your strategy call this commit function once the rest of the message
processing has been done.

The offset to be committed in Kafka is always the next offset to be consumed from, i.e. message's offset + 1.
In Arroyo, this means you should commit `Message.next_offset` and never `Message.offset` when done processing
that message. Arroyo exposes `Message.position_to_commit` to make this easier.

It is not safe to commit every offset in a high throughput consumer as this will add a lot of load to the system.
Commits should generally be throttled. `CommitPolicy` is the Arroyo way of specifying commit frequency. A `CommitPolicy`
must be passed to the stream processor, which allows specifying a minimum commit frequency (or messages between commits).
Commit throttling can be skipped when needed (i.e. during consumer shutdown) by passing `force=True` to the commit callback.
If you are not sure how often to commit, `ONCE_PER_SECOND` is a reasonable option.

.. code-block:: Python

    class ConsumerStrategy(ProcessingStrategy[KafkaPayload]):
        def __init__(
            self,
            committer: Commit,
            partitions: Mapping[Partition, int],
        ):
            self.__commit = commit

        def poll(self) -> None:
            pass

        def submit(self, message: Message[KafkaPayload]) -> None:
            # do something (synchronous) with the message
            do_something()
            self.__commit(
                {message.partition: message.position_to_commit}
            )

        def close(self) -> None:
            pass

        def terminate(self) -> None:
            print("Terminating")

        def join(self, timeout: Optional[float] = None) -> None:
            pass
