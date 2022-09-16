# Arroyo

`Arroyo` is a Python library for working with streaming data.

Most of the code here has been extracted from `Snuba` so that it can be reused in `Sentry` and other services.

Arroyo provides:

* Consumer and producer interfaces. The primary use case is for working with Apache Kafka streams, however it also supports custom backends and includes local (memory or file based) consumer and producer implementations
* Consumer strategy interface that helps build the processing strategy for how raw messages that are consumed should be filtered, transformed, batched and flushed as required

Official documentation: https://getsentry.github.io/arroyo/
