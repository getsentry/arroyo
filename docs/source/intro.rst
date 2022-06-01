Arroyo is a library to build streaming applications that consume from
and produce to Kafka.

It relies on the `confluent_kafka` python library, which itself relies
on `librdkafka`.

Arroyo provides mainly three functionalities:

* A set of abstractions inspired to common messaging applications patterns.
* Some abstractions to simplify offset management and rebalancing.
* An in memory broker abstraction to simplify writing unit tests.


Getting Started
***************

Install it with pip.

.. code-block:: python

   pip install sentry-arroyo

Find some `examples of usage <https://github.com/getsentry/arroyo/tree/main/examples>`_.
