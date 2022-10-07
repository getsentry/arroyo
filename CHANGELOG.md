# Changelog and versioning

## 1.0.6

### Various fixes & improvements

- feat: Introduce commit policies (#106) by @lynnagara

## 1.0.5

### Various fixes & improvements

- Add parallel collect timeout (#104) by @fpacifici
- move 'what is arroyo for' docs to public docs (#103) by @volokluev
- Link the official documentation to the readme (#102) by @fpacifici
- fix(docs) Fix makefile (#101) by @fpacifici
- Fix branch name in docs generation (#100) by @fpacifici
- feat(docs) Add a `getting started` page to the docs (#99) by @fpacifici

## 1.0.4

### Various fixes & improvements

- fix: Use correct log level for fatal crashes [sns-1622] (#97) by @untitaker

## 1.0.3

### Various fixes & improvements

- adjust confluent-kafka version to >= (#96) by @asottile-sentry

## 1.0.2

### Various fixes & improvements

- upgrade confluent-kafka to 1.9.0 (#95) by @asottile-sentry
- Configure CodeQL (#94) by @mdtro

## 1.0.1

### Various fixes & improvements

- deps: Update confluent-kafka-python to 1.8.2 (#92) by @lynnagara
- doc: Remove reference to synchronized consumer in readme (#93) by @lynnagara

## 1.0.0

### Breaking changes in this release

- Synchronized consumer deprecated (#81)
- create deprecated in favor of create_with_partitions (#91)

## 0.2.2

### Various fixes & improvements

- fix(dlq): InvalidMessages Exception __repr__ (#88) by @rahul-kumar-saini
- feat: Add docker-compose orchestrated example script (#85) by @cmanallen
- fix(dlq): Join method did not handle InvalidMessages (#83) by @rahul-kumar-saini

## 0.2.1

### Various fixes & improvements

- always use create_with_partitions (#82) by @MeredithAnya

## 0.2.0

### Various fixes & improvements

- feat(processing): Pass through partitions using create_with_partitions (#75) by @MeredithAnya
- ref: Upgrade Kafka and Zookeeper in CI (#80) by @lynnagara

## 0.1.1

### Various fixes & improvements

- ref: don't depend on mypy, upgrade and fix mypy (#79) by @asottile-sentry
- docs: Fix docstring (#77) by @lynnagara

## 0.1.0

### Various fixes & improvements

- feat: Prevent passing invalid next_offset (#73) by @lynnagara
- fix: Fix offset committed in example (#76) by @lynnagara

## 0.0.21

### Breaking changes

- refactor(dlq): Renamed policy "closure" to policy "creator" (#71) by @rahul-kumar-saini

## 0.0.20

### Various fixes & improvements

- fix(dlq): Policy closure added + Producer closed (#68) by @rahul-kumar-saini

## 0.0.19

### Various fixes & improvements

- fix(dlq): Updated DLQ logic for ParallelTransformStep (#61) by @rahul-kumar-saini

## 0.0.18

### Various fixes & improvements

- fix(tests): Make tests pass on M1 Macs (#67) by @mcannizz
- test: Fix flaky test (#66) by @lynnagara
- feat: Add flag to restore confluence kafka auto.offset.reset behavior (#54) by @mitsuhiko
- feat(dlq): Added produce policy (#57) by @rahul-kumar-saini
- feat(dlq): Revamped Invalid Message(s) Model (#64) by @rahul-kumar-saini

## 0.0.17

### Various fixes & improvements

- feat: Avoid unnecessarily recreating processing strategy (#62) by @lynnagara
- feat: Run CI on multiple Python versions (#63) by @lynnagara

## 0.0.16

### Various fixes & improvements

- feat: Support incremental assignments in stream processor (#58) by @lynnagara

## 0.0.15

### Various fixes & improvements

- feat(dlq): InvalidMessage exception refactored to handle multiple invalid messages (#50) by @rahul-kumar-saini
- test: Fix flaky test (#59) by @lynnagara
- feat(consumer): Wrap consumer strategy with DLQ if it exists (#56) by @rahul-kumar-saini

## 0.0.14

### Various fixes & improvements

- feat: Bump confluent-kafka-python to 1.7.0 (#55) by @lynnagara

## 0.0.13

### Various fixes & improvements

- feat(consumer): Support incremental cooperative rebalancing (#53) by @lynnagara

## 0.0.12

### Various fixes & improvements

- feat: Bump confluent kafka to 1.6.1 (#51) by @lynnagara
- ci: Upgrade black version to 22.3.0 (#52) by @lynnagara

## 0.0.11

### Various fixes & improvements

- Removed Generic payload from DLQ Policy (#49) by @rahul-kumar-saini
- export InvalidMessage from DLQ (#48) by @rahul-kumar-saini

## 0.0.10

### Various fixes & improvements

- Dead Letter Queue (#47) by @rahul-kumar-saini
- Added an example for Arroyo usage. (#44) by @rahul-kumar-saini

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
