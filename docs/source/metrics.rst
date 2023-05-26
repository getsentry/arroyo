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

   from arroyo.utils.metrics import Metrics, MetricName

    class MyMetrics(Metrics):
        def increment(
            self, name: MetricName, value: Union[int, float] = 1, tags: Optional[Tags] = None
        ) -> None:
            # Increment a counter by the given value.
            record_incr(name, value, tags)

        def gauge(
            self, name: MetricName, value: Union[int, float], tags: Optional[Tags] = None
        ) -> None:
            # Sets a gauge metric to the given value.
            record_gauge(name, value, tags)

        def timing(
            self, name: MetricName, value: Union[int, float], tags: Optional[Tags] = None
        ) -> None:
            # Emit a timing metric with the given value.
            record_timing(name, value, tags)

    metrics_backend = MyMetrics()

    configure_metrics(metrics_backend)


Available Metrics
====================

.. literalinclude:: ../../arroyo/utils/metric_defs.py

API
=======

.. automodule:: arroyo.utils.metrics
   :members:
   :undoc-members:
