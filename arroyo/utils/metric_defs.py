from typing import Literal

MetricName = Literal[
    # Number of messages in a multiprocessing batch
    "arroyo.strategies.run_task_with_multiprocessing.batch.size.msg",
    # Number of bytes in a multiprocessing batch
    "arroyo.strategies.run_task_with_multiprocessing.batch.size.bytes",
    # Number of messages in a multiprocessing batch after the message transformation
    "arroyo.strategies.run_task_with_multiprocessing.output_batch.size.msg",
    # Number of bytes in a multiprocessing batch after the message transformation
    "arroyo.strategies.run_task_with_multiprocessing.output_batch.size.bytes",
    # Number of times the consumer is spinning
    "arroyo.consumer.run.count",
    # Number of times the consumer encounted an invalid message.
    "arroyo.consumer.invalid_message.count",
    # How long it took the Reduce step to fill up a batch
    "arroyo.strategies.reduce.batch_time",
    # Counter, incremented when a strategy after multiprocessing applies
    # backpressure to multiprocessing. May be a reason why CPU cannot be
    # saturated.
    "arroyo.strategies.run_task_with_multiprocessing.batch.backpressure",
    # Counter, incremented when multiprocessing cannot fill the input batch
    # because not enough memory was allocated. This results in batches smaller
    # than configured. Increase `input_block_size` to fix.
    "arroyo.strategies.run_task_with_multiprocessing.batch.input.overflow",
    # Counter, incremented when multiprocessing cannot pull results in batches
    # equal to the input batch size, because not enough memory was allocated.
    # This can be devastating for throughput. Increase `output_block_size` to
    # fix.
    "arroyo.strategies.run_task_with_multiprocessing.batch.output.overflow",
    # Arroyo has decided to re-allocate a block in order to combat input buffer
    # overflow. This behavior can be disabled by explicitly setting
    # `input_block_size` to a not-None value in `RunTaskWithMultiprocessing`.
    "arroyo.strategies.run_task_with_multiprocessing.batch.input.resize",
    # Arroyo has decided to re-allocate a block in order to combat output buffer
    # overflow. This behavior can be disabled by explicitly setting
    # `output_block_size` to a not-None value in `RunTaskWithMultiprocessing`.
    "arroyo.strategies.run_task_with_multiprocessing.batch.output.resize",
    # How many batches are being processed in parallel by multiprocessing.
    "arroyo.strategies.run_task_with_multiprocessing.batches_in_progress",
    # Counter. A subprocess by multiprocessing unexpectedly died.
    "sigchld.detected",
    # Gauge. Shows how many processes the multiprocessing strategy is
    # configured with.
    "arroyo.strategies.run_task_with_multiprocessing.processes",
    # Time (unitless) spent polling librdkafka for new messages.
    "arroyo.consumer.poll.time",
    # Time (unitless) spent in strategies (blocking in strategy.submit or
    # strategy.poll)
    "arroyo.consumer.processing.time",
    # Time (unitless) spent pausing the consumer due to backpressure (MessageRejected)
    "arroyo.consumer.backpressure.time",
    # Time (unitless) spent in handling `InvalidMessage` exceptions and sending
    # messages to the the DLQ.
    "arroyo.consumer.dlq.time",
    # Time (unitless) spent in waiting for the strategy to exit, such as during
    # shutdown or rebalancing.
    "arroyo.consumer.join.time",
    # Time (unitless) spent in librdkafka callbacks. This metric's timings
    # overlap other timings, and might spike at the same time.
    "arroyo.consumer.callback.time",
    # Time (unitless) spent in shutting down the consumer. This metric's
    # timings overlap other timings, and might spike at the same time.
    "arroyo.consumer.shutdown.time",
    # Consumer latency in seconds. Recorded by the commit offsets strategy.
    "arroyo.consumer.latency",
    # Queue size of background queue that librdkafka uses to prefetch messages.
    "arroyo.consumer.librdkafka.total_queue_size",
    # Counter metric to measure how often the healthcheck file has been touched.
    "arroyo.processing.strategies.healthcheck.touch",
    # Number of messages dropped in the FilterStep strategy
    "arroyo.strategies.filter.dropped_messages",
]
