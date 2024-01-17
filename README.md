# Arroyo

<p align="center">
    <!-- do not specify height so that it scales proportionally on mobile -->
    <img src=docs/source/_static/arroyo-banner.png width=583 />
</p>

`Arroyo` is a library to build streaming applications that consume from and produce to Kafka.

Arroyo consists of three components:

* Consumer and producer backends
    - The Kafka backend is a wrapper around the librdkafka client, and attempts to simplify rebalancing and offset management even further
    - There is also an in memory and a file based consumer and producer implementation that can be used for testing
* A strategy interface
    - Arroyo includes a number of pre-built strategies such as `RunTask`, `Filter`, `Reduce`, `CommitOffsets` and more.
    - Users can write their own strategies, though in most cases this should not be needed as the library aims to provide generic, reusable strategies that cover most stream processing use cases
    - Strategies can be chained together to form complex message processing pipelines.
* A streaming engine which manages the relationship between the consumer and strategies
    - The `StreamProcessor` controls progress by the consumer and schedules work for execution by the strategies.

All documentation is in the `docs` directory. It is hosted at https://getsentry.github.io/arroyo/ and can be built locally by running `make docs`
