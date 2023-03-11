==================
Metrics
==================


Arroyo consumers and strategies attempt to auto instrument some metrics that most people find useful
to understand the behavior and performance of their consumers. These metrics are typically sampled or
buffered as appropriate and flushed periodically (often once per second).

In order to use these metrics, you must configure a metrics backend that conforms to the metrics protocol
before creating your consumer.

This can be done like so:

.. code:: python

    class Metrics:
        def increment(
            self, name: str, value: Union[int, float] = 1, tags: Optional[Tags] = None
        ) -> None:
            # Increment a counter by the given value.
            pass

        def gauge(
            self, name: str, value: Union[int, float], tags: Optional[Tags] = None
        ) -> None:
            # Sets a gauge metric to the given value.
            pass

        def timing(
            self, name: str, value: Union[int, float], tags: Optional[Tags] = None
        ) -> None:
            # Emit a timing metric with the given value.
            pass

    metrics_backend = Metrics()

    configure_metrics(metrics_backend)

Some of the metrics emitted by Arroyo include:

.. list-table:: Metrics
   :widths: 25 25 50
   :header-rows: 1

   * - Metric name
     - Type
     - Description
   * - arroyo.consumer.poll.time
     - Timing
     - Time spent polling for messages. A higher number means the consumer has headroom as it is waiting for messages to arrive.
   * - arroyo.consumer.processing.time
     - Timing
     - Time spent processing messages. A higher number means the consumer spends more time processing messages.
   * - arroyo.consumer.paused.time
     - Timing
     - Time spent in backpressure. Usually means there could be some processing bottleneck.


.. automodule:: arroyo.utils.metrics
   :members:
   :undoc-members:
