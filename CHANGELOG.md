# Changelog and versioning

## 0.0.9

### Various fixes & improvements

- ref(metrics): Add metrics for time spent polling and closing batch (#46) by @nikhars

## 0.0.8

### Various fixes & improvements

- chore(parallel_collect): Allow importing ParallelCollectStep (#43) by @nikhars

## 0.0.7

### Various fixes & improvements

- perf(collect): Add ParallelCollect step (#41) by @nikhars

## 0.0.6

### Various fixes & improvements

- Increase log level (#39) by @fpacifici
- feat(perf) Add latency metrics to the messages coming from the commit log (#38) by @fpacifici

## 0.0.5

- Number of processes in the multi-process poll metric added. Its key is
  `transform.processes`.

## 0.0.4

- Handle missing `orig_message_ts` header. Since all events in the pipeline produced using an older version of arroyo may not have the header yet, temporarily support a None value for `orig_message_ts`.

## 0.0.3

- Replaces Offset in consumer and processing strategy with Position, which contains both offset and timestamp information. `stage_offsets` is now `stage_positions` and `commit_offsets` is now `commit_positions`, and now includes the timestamp.

- Add orig_message_ts field to Commit and commit_codec. This field is included in the Kafka payload as a header.

## 0.0.2

- Add optional initializer function to parallel transform step. Supports passing a custom function to be run on multiprocessing pool initialization.

## 0.0.1

- First release 🎉

## Versioning Policy

This project follows [semver](https://semver.org/), with three additions:

- Semver says that major version ``0`` can include breaking changes at any time. Still, it is common practice to assume that only ``0.x`` releases (minor versions) can contain breaking changes while ``0.x.y`` releases (patch versions) are used for backwards-compatible changes (bugfixes and features). This project also follows that practice.

- All undocumented APIs are considered internal. They are not part of this contract.

- Certain features may be explicitly called out as "experimental" or "unstable" in the documentation. They come with their own versioning policy described in the documentation.
