# Changelog and versioning

## 2.10.3

### Various fixes & improvements

- docs: Organise processing strategies and add to TOC (#226) by @lynnagara
- ref: Remove legacy DLQ implementation (#225) by @lynnagara
- docs: Remove WIP note in DLQ docs (#224) by @lynnagara
- docs: 2023 (#223) by @lynnagara
- feat: Count how many times consumer spins (#222) by @untitaker
- fix: Add metrics around RunTaskWithMultiprocessing + docs [SNS-2204] (#220) by @untitaker
- ref: Clean up documentation (#221) by @untitaker

## 2.10.2

### Various fixes & improvements

- fix: Fix RunTaskInThreads to handle InvalidMessage during close, add tests (#218) by @lynnagara
- fix(dlq): Fix crash during join when InvalidMessage in flight (#217) by @lynnagara
- feat: Measure time to join strategy [SNS-2154] (#214) by @untitaker
- feat: Add error instrumentation and timing to arroyo callbacks (#215) by @untitaker
- feat: Add consumer dlq time (#213) by @untitaker

## 2.10.1

### Various fixes & improvements

- fix: Do not enter invalid state when strategy.submit raises InvalidMessage (#212) by @untitaker

## 2.10.0

### Various fixes & improvements

- feat(dlq): A DLQ implementation that keeps a copy of raw messages in a buffer (#202) by @lynnagara
- fix: Fix installation of sphinx-autodoc-typehints (#211) by @untitaker

## 2.9.1

### Various fixes & improvements

- Revert "fix: Prevent repeated calls to consumer.pause() if already paused (#209)" (#210) by @lynnagara

## 2.9.0

### Various fixes & improvements

- feat: Codecs can encode and be used outside of strategies (#208) by @lynnagara
- fix: Prevent repeated calls to consumer.pause() if already paused (#209) by @lynnagara
- feat: Make DLQ buffer size configurable (#207) by @lynnagara
- ref: Remove the FileMessageStorage backend (#206) by @lynnagara

## 2.8.0

### Various fixes & improvements

- feat: DLQ perf test (#205) by @lynnagara
- ref: Split run task, update strategies docs page (#204) by @lynnagara
- fix: RunTaskWithMultiprocessing support for filtered message (#203) by @lynnagara
- feat: Add unfold strategy (#197) by @lynnagara
- feat(dlq): Make dlq interface generic (#200) by @lynnagara
- feat(dlq): Introduce DLQ docs and interfaces (#195) by @lynnagara
- docs: Document how to use metrics (#198) by @lynnagara
- docs: Update get started section (#196) by @lynnagara
- meta: Update readme to remove some duplicate text (#199) by @lynnagara
- ci: Make tests go faster (#194) by @lynnagara
- docs: Update readme (#192) by @lynnagara
- small grammar improvements (#191) by @barkbarkimashark

## 2.7.1

### Various fixes & improvements

- fix(run_task): Fix bug where join() would not actually wait for tasks (#190) by @untitaker

## 2.7.0

### Various fixes & improvements

- fix(filter-strategy): Options to commit filtered messages, take 2 (#185) by @untitaker
- fix: Fix multiprocessing join (#189) by @lynnagara
- build: Run CI on Python 3.11 (#188) by @lynnagara

## 2.6.0

### Various fixes & improvements

- feat: Configurable strategy join timeout (#187) by @lynnagara
- feat: Log exception if processing strategy exists on assignment (#186) by @lynnagara

## 2.5.3

### Various fixes & improvements

- fix(decoder): Fix importing optional dependencies (#184) by @lynnagara

## 2.5.2

- No documented changes.

## 2.5.1

### Various fixes & improvements

- fix(produce): Fix closing next step (#183) by @lynnagara
- fix(reduce): Ensure next step is properly closed (#182) by @lynnagara
- ref: Don't alias ProcessingStrategy as ProcessingStep (#181) by @lynnagara
- ref: upgrade isort to work around poetry breakage (#179) by @asottile-sentry
- fix: Documentation typos (#178) by @markstory
- build: Split avro, json, msgpack into separate modules (#176) by @lynnagara

## 2.5.0

### Various fixes & improvements

- feat: Remove CollectStep and ParallelCollectStep (#173) by @lynnagara
- feat(reduce): Record `arroyo.strategies.reduce.batch_time` (#175) by @lynnagara
- docs: Add decoders section (#174) by @lynnagara
- build: Make json, msgpack and avro optional dependencies (#172) by @lynnagara

## 2.4.0

### Various fixes & improvements

- feat(serializers): Add more decoders (#171) by @lynnagara
- feat(reduce): Support greater flexibility in initial value (#170) by @lynnagara
- auto publish to internal pypi on release (#169) by @asottile-sentry
- feat: Remove support for cooperative-sticky partition assignment strategy (#168) by @lynnagara

## 2.3.1

### Various fixes & improvements

- ref(decoder): Remove dead code (#167) by @lynnagara
- ref(commit_policy): Improve performance of commit policy (#162) by @untitaker
- feat(decoders): Json decoder can be used without schema (#166) by @lynnagara

## 2.3.0

### Various fixes & improvements

- feat: Add Reduce strategy (#157) by @lynnagara
- ref: Remove ``Position`` and stop passing timestamps to the commit function (#165) by @lynnagara
- feat: Add a  schema validation strategy (#154) by @lynnagara
- fix: Ensure Collect/ParallelCollect properly calls next_step methods (#163) by @lynnagara
- feat: Remove the BatchProcessingStrategy and AbstractBatchWorker (#164) by @lynnagara
- test: Fix streamprocessor tests (#160) by @lynnagara
- docs: Call out the run task strategy (#161) by @lynnagara
- fix: RunTaskWithMultiprocessing handles MessageRejected from subsequent steps (#158) by @lynnagara
- ref: Improve typing of CommitOffsets strategy (#159) by @lynnagara

## 2.2.0

### Various fixes & improvements

- fix(commit_policy): Calculate elapsed timerange correctly (#155) by @untitaker
- ref: Remove dead code (#153) by @lynnagara
- feat(Collect) Attempt to redesign the Collector step (#127) by @fpacifici

## 2.1.0

### Various fixes & improvements

- feat: Collect / ParallelCollect supports a next step (#149) by @lynnagara
- feat: Add RunTaskWithMultiprocessing strategy (#152) by @lynnagara
- feat: Add replace() method to message (#151) by @lynnagara
- feat: Death to the consumer strategy factory (#150) by @lynnagara
- feat: Add RunTask strategy (#147) by @lynnagara

## 2.0.0

### Various fixes & improvements

- feat: Split the Message interface to better support batching steps (#134) by @lynnagara
- feat: Remove BatchProcessingStrategyFactory from Arroyo (#138) by @lynnagara
- docs: Add docstring for MessageRejected (#146) by @lynnagara
- feat: Increase log level during consumer shutdown (#144) by @lynnagara
- feat: Remove ProduceAndCommit (#145) by @lynnagara

## 1.2.0

### Various fixes & improvements

- feat: Introduce separate commit strategy (#140) by @lynnagara
- feat: Mark ConsumerStrategyFactory/KafkaConsumerStrategyFactory deprecated (#139) by @lynnagara
- docs: Update DLQ docs (#136) by @lynnagara
- fix: Compute offset deltas for commit policy [SNS-1863] (#135) by @untitaker
- fix: Make Position pickleable (#129) by @lynnagara
- feat: Avoid storing entire messages in RunTaskInThreads (#133) by @lynnagara
- feat: Avoid storing entire messages in the produce step (#132) by @lynnagara
- test: Split the collect and transform tests into separate files (#130) by @lynnagara
- test: Simplify the commit codec test (#131) by @lynnagara

## 1.1.0

### Various fixes & improvements

- Update actions/upload-artifact to v3.1.1 (#128) by @mattgauntseo-sentry
- feat: Add stream processor metrics (#124) by @lynnagara
- docs: Remove comment about asyncio in architecture (#121) by @lynnagara
- docs: Remove the big pointless black square (#120) by @lynnagara
- docs: Remove invalid release number (#122) by @lynnagara
- feat: Make commit policy a required argument (#116) by @lynnagara
- docs(autodocs) Adds autodoc support and import docstrings in the sphinx doc (#118) by @fpacifici
- Add architecture page (#117) by @fpacifici
- feat: Add RunTaskInThreads strategy (#111) by @lynnagara
- feat: Add produce step to library (#109) by @lynnagara
- ref: Rename variable (#115) by @lynnagara

## 1.0.7

### Various fixes & improvements

- ref: Move strategies out of streaming module (#114) by @lynnagara
- feat: Add Message.position_to_commit (#113) by @lynnagara
- feat: Simplify example (#112) by @lynnagara
- feat: Add deprecation comment on batching strategies (#110) by @lynnagara
- build: Run CI on Python 3.10 (#108) by @lynnagara
- build: Include requirements.txt in source distribution (#107) by @lynnagara

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
