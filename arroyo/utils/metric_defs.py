from typing import Literal

MetricName = Literal[
    # Number of messages in a multiprocessing batch
    "arroyo.strategies.run_task_with_multiprocessing.batch.size.msg",
    # Number of bytes in a multiprocessing batch
    "arroyo.strategies.run_task_with_multiprocessing.batch.size.bytes",
    # Number of times the consumer is spinning
    "arroyo.consumer.run.count",
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
    "arroyo.consumer.paused.time",
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
]
