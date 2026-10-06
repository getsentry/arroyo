Backpressure
============

.. py:currentmodule:: arroyo.processing.strategies

Arroyo's own processing strategies internally apply backpressure by raising
:py:class:`~abstract.MessageRejected`. Most
consumers do not require additional work to deal with backpressure correctly.

If you want to slow down the consumer based on some external signal or
condition, you can achieve that most effectively by raising the same exception
from within a callback passed to :py:class:`~run_task.RunTask` while the
consumer is supposed to be paused

.. code-block:: Python

    class ConsumerStrategyFactory(ProcessingStrategyFactory[KafkaPayload]):
        def __init__(self):
            self.is_paused = False

        def create_with_partitions(
            self,
            commit: Commit,
            partitions: Mapping[Partition, int],
        ) -> ProcessingStrategy[KafkaPayload]:
            def handle_message(message: Message[KafkaPayload]) -> Message[KafkaPayload]:
                if self.is_paused:
                    raise MessageRejected()

                print(f"MSG: {message.payload}")
                return message

            return RunTask(handle_message, CommitOffsets(commit))

It is not recommended to apply backpressure by just ``sleep()``-ing in
:py:class:`~abstract.ProcessingStrategy.submit` (or, in this example,
``handle_message``) for more than a few milliseconds. While this definitely
pauses the consumer, it will block the main thread for too long and and prevent
things like consumer rebalancing from occuring.

A 0.01 second sleep is applied each time :py:class:`~abstract.MessageRejected` is
raised to prevent the main thread spinning at 100% CPU. However background thread
performance may be impacted during this time.
