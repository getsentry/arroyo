# Changelog and versioning

## 2.18.1

### Various fixes & improvements

- ref(batching): add compute_batch_size to BatchStep (#390) by @MeredithAnya
- ref(rust/run_task): Remove unnecessary boxing, make FnMut, change return type (#388) by @untitaker

## 2.18.0

### Various fixes & improvements

- fix: Allow sending committable from Unfold (#371) by @mj0nez
- all-repos: update actions/upload-artifact to v4 (#381) by @joshuarli

## 2.17.6

### Various fixes & improvements

- Remove non-existent @getsentry/processing from CODEOWNERS (#386) by @onkar

## 2.17.5

### Various fixes & improvements

- chore: Fix release builds (#385) by @untitaker
- Add a basic metric for tracking the capacity in VecDeque buffer (#383) by @ayirr7
- feat: enhance metrics defs (#378) by @mj0nez
- feat: Add From<BrokerMessage<_>> impl for InvalidMessage (#377) by @evanpurkhiser
- feat: Add Noop processing strategy (#376) by @evanpurkhiser
- Update RunTask to receive a Message<TPayload> (#375) by @evanpurkhiser
- hotfix, fix master ci (66f1efc3) by @untitaker
- fix: add guard to Produce.poll to ensure next_step is called regardless of produce queue (#370) by @mj0nez
- ref: Add pre-commit hook for rustfmt (#364) by @untitaker
- update accumulator sig to return Result<TResult> instead of TResult (#359) by @john-z-yang
- ref: Use coarsetime consistently (#366) by @untitaker
- ref(rust): Backpressure metrics for threadpools (#367) by @untitaker
- ref(reduce): Refactor for timeout=0 (#363) by @untitaker
- ref(rust): Remove strategy.close (#361) by @untitaker
- ref(rust): Add join-time metric for threadpools (#362) by @untitaker

## 2.17.4

### Various fixes & improvements

- rust: add more rust logging (#351) by @dbanda
- fixes #353: return message.payload (#354) by @mwarkentin

## 2.17.3

### Various fixes & improvements

- feat(header): Implement find method on headers (#350) by @nikhars

## 2.17.2

### Various fixes & improvements

- feat: make default auto.offset.reset earliest (#349) by @lynnagara

## 2.17.1

### Various fixes & improvements

- fix: Enable stats collection (#348) by @phacops

## 2.17.0

### Various fixes & improvements

- ref(reduce): Allow to reduce to non-cloneable values (#346) by @untitaker

## 2.16.5

### Various fixes & improvements

- build(deps): bump black from 22.3.0 to 24.3.0 (#343) by @dependabot
- feat: confluent-kafka-python 2.3.0 (#344) by @lynnagara
- meta: Update codeowners (#345) by @lynnagara

## 2.16.4

### Various fixes & improvements

- ref: Metric definition (#341) by @lynnagara
- feat: Ingest a metric for rdkafka queue size (#342) by @phacops

## 2.16.2

### Various fixes & improvements

- feat: Increase backpressure threshold to 5 seconds (#340) by @lynnagara

## 2.16.1

### Various fixes & improvements

- ref(metrics): Add pause/resume counters [INC-626] (#338) by @untitaker
- perf: inline now calling `coarsetime::Instant` (#336) by @anonrig

## 2.16.0

### Various fixes & improvements

- Add flexible Reduce strategy type (#333) by @cmanallen
- fix(release): Stop releasing to crates.io while it doesn't work (#335) by @untitaker
- feat(logs): Add partition info upon assignment and revocation (#334) by @ayirr7
- ref: Restore owners on rust-arroyo (#330) by @untitaker
- fix: Rename Rust arroyo metric since it has a different unit (#329) by @untitaker
- fix(rust): Add feature to Reduce to flush empty batches (#332) by @untitaker
- ref: Add release workflow for Rust (#327) by @untitaker
- ref(ci): Add Rust CI (#324) by @untitaker
- Revert "ref: Move rust-arroyo from snuba to arroyo" (#325) by @untitaker
- ref: Move rust-arroyo from snuba to arroyo (#323) by @untitaker
- move rust-arroyo to subdirectory (#323) by @untitaker
- fix(multiprocessing): Implement better error messages for block overflow (#322) by @untitaker
- rust: add rust concurrency metric (#5341) by @untitaker
- ref(rust): Don't panic in RunTaskInThreads::poll (#5387) by @untitaker
- deps(rust): Change rdkafka dep to upstream master (#5386) by @untitaker
- ref(metrics): Refactor how global tags work, and introduce min_partition tag (#5346) by @untitaker
- Add Metrics impl based on `merni` (#5351) by @untitaker
- fix: Remove any panics in threads (#5353) by @untitaker
- ref(devserver): Use Rust consumers almost everywhere, and fix commitlog implementation (#5311) by @untitaker
- Avoid calling `Topic::new` for every received Message (#5331) by @untitaker
- Reuse Tokio Handle in DlqPolicy (#5329) by @untitaker
- ref(rust): Log actual error if strategy panics (#5317) by @untitaker
- fix(rust): add testcase for empty batches (#5299) by @untitaker
- Revert "ref(rust): Do not do extra work when merging if not needed (#5294)" (#323) by @untitaker

_Plus 146 more_

## 2.15.3

### Various fixes & improvements

- fix: Make `arroyo.consumer.latency` a timing metric (#316) by @lynnagara

## 2.15.2

### Various fixes & improvements

- fix(dlq): Ensure consumer crashes if DLQ limit is reached (#314) by @lynnagara
- fix(multiprocessing): Reset pool if tasks are not completed (#315) by @lynnagara

## 2.15.1

### Various fixes & improvements

- fix(filter): Handle MessageRejected correctly (INC-584) (#313) by @untitaker

## 2.15.0

### Various fixes & improvements

- feat: Reusable multiprocessing pool (#311) by @lynnagara
- ref: Combine DlqLimitState methods (#312) by @loewenheim

## 2.14.25

### Various fixes & improvements

- ref: Add more metrics for slow rebalancing (#310) by @untitaker

## 2.14.24

### Various fixes & improvements

- fix(dlq): Actually respect the DLQ limits (#309) by @lynnagara

## 2.14.23

### Various fixes & improvements

- feat: Enable automatic latency recording for consumers (#308) by @lynnagara

## 2.14.22

### Various fixes & improvements

- fix: Default input block size should be larger (#306) by @untitaker

## 2.14.21

### Various fixes & improvements

- fix: Attempt to fix InvalidStateError (#305) by @lynnagara

## 2.14.20

### Various fixes & improvements

- ref: Rename backpressure metric (#303) by @lynnagara

## 2.14.19

### Various fixes & improvements

- ref: Add extra logging for debugging commits (#302) by @lynnagara

## 2.14.18

### Various fixes & improvements

- fix(dlq): RunTaskWithMultiprocessing supports forwarding downstream invalid message (#301) by @lynnagara

## 2.14.17

### Various fixes & improvements

- fix: Cleanup backpressure state between assignments (#299) by @lynnagara

## 2.14.16

### Various fixes & improvements

- fix: Temporarily bring back support for legacy commit log format (#298) by @lynnagara

## 2.14.15

### Various fixes & improvements

- fix: Revert change when consumer is paused (#297) by @lynnagara

## 2.14.14

### Various fixes & improvements

- perf: Avoid unnecessarily clearing the rdkafka buffer on backpressure (#296) by @lynnagara

## 2.14.13

### Various fixes & improvements

- feat: Add optional received_p99 timestamp to commit log (#295) by @lynnagara

## 2.14.12

### Various fixes & improvements

- doc: Describe max_batch_size/max_batch_time in our strategies (#294) by @untitaker
- ref: Timestamp is not optional on commit log (#293) by @lynnagara
- feat(metrics): Add a metric for number of invalid messages encountered (#292) by @lynnagara

## 2.14.11

### Various fixes & improvements

- fix(dlq): Gracefully handle case of no valid messages (#291) by @nikhars

## 2.14.10

### Various fixes & improvements

- ref: Use float in commit codec instead of complex datetime format (#290) by @lynnagara

## 2.14.9

### Various fixes & improvements

- ref: A better commit log format (#289) by @lynnagara

## 2.14.8

### Various fixes & improvements

- Revert "Add coverage report to CI (#286)" (#288) by @lynnagara
- feat: Remove pointless (and wrong) config check (#287) by @lynnagara
- Add coverage report to CI (#286) by @dbanda
- Make producer thread a deamon (#282) by @dbanda

## 2.14.7

### Various fixes & improvements

- fix(dlq): Make InvalidMessage pickleable (#284) by @nikhars

## 2.14.6

### Various fixes & improvements

- fix(produce): Apply backpressure instead of crashing (#281) by @lynnagara
- feat: Automatically resize blocks if they get too small (#270) by @untitaker

## 2.14.5

### Various fixes & improvements

- fix: Ensure carried over message is in buffer (#283) by @lynnagara
- docs: Actually fix the getting started (#280) by @lynnagara

## 2.14.4

### Various fixes & improvements

- feat: Add global shutdown handler for strategy factory (#278) by @untitaker

## 2.14.3

### Various fixes & improvements

- fix(run_task_in_threads): Commit offsets even when consumer is idle (#276) by @untitaker
- add socket.timeout.ms to supported kafka configurations (#275) by @hubertdeng123
- release: 2.14.2 (5f8ee08a) by @getsentry-bot

## 2.14.2

- No documented changes.

## 2.14.1

### Various fixes & improvements

- fix(reduce): Add missing call to next_step.terminate() (#272) by @untitaker
- metrics: Add metrics about dropped messages in FilterStep (#265) by @ayirr7
- fix: Logo dimensions on mobile (#268) by @untitaker
- doc: Update dlq doc (#267) by @lynnagara
- docs: Update goals (#266) by @lynnagara
- doc: Add new logo (#264) by @untitaker

## 2.14.0

### Various fixes & improvements

- ref: Remove deprecated strategies (#263) by @lynnagara
- feat: Add liveness healthcheck (#262) by @untitaker

## 2.13.0

### Various fixes & improvements

- feat: Bump confluent-kafka-python (#258) by @lynnagara

## 2.12.1

### Various fixes & improvements

- fix: Commit offsets even when topic is empty (#259) by @untitaker

## 2.12.0

### Various fixes & improvements

- feat: A better default retry policy (#256) by @lynnagara

## 2.11.7

### Various fixes & improvements

- feat: Avoid spinning on MessageRejected (#253) by @lynnagara

## 2.11.6

### Various fixes & improvements

- feat: Basic support for librdkafka stats (#252) by @untitaker

## 2.11.5

### Various fixes & improvements

- add entries to list (#250) by @ayirr7
- ref(multiprocessing): Rename some metrics and add documentation (#241) by @untitaker
- feat(metrics): Allow reconfiguring the metrics backend (#249) by @lynnagara
- ref: Global metrics registry (#245) by @untitaker

## 2.11.4

### Various fixes & improvements

- fix: Fix run task in threads (again) (#244) by @lynnagara

## 2.11.3

### Various fixes & improvements

- fix(run-task-in-threads): Fix shutdown (#243) by @lynnagara
- fix: Mermaid diagrams in dark mode (#242) by @untitaker

## 2.11.2

### Various fixes & improvements

- Revert "feat: add more item size stats to multiprocessing (and metrics refactor) (#235)" (#240) by @untitaker
- fix(multiprocessing): More performant backpressure [INC-378] (#238) by @untitaker
- fix(multiprocessing): Honor join() timeout even if processing pool is overloaded [INC-378] (#237) by @untitaker
- docs: Minor readability details (#236) by @kamilogorek
- feat: add more item size stats to multiprocessing (and metrics refactor) (#235) by @untitaker

## 2.11.1

### Various fixes & improvements

- fix: Distinguish between backpressure and output overflow (#234) by @untitaker

## 2.11.0

### Various fixes & improvements

- perf(dlq): Improve performance when there are many invalid messages (#233) by @lynnagara

## 2.10.6

### Various fixes & improvements

- chore(processor): More logs for shutdown sequences (#230) by @nikhars
- fix: Remove flaky test (#232) by @untitaker
- fix: Do not log error when consumer is already crashing (#228) by @untitaker

## 2.10.5

### Various fixes & improvements

- fix(run_task_with_multiprocessing): Do not attempt to skip over invalid messages (#231) by @untitaker

## 2.10.4

### Various fixes & improvements

- chore(processor): Add more logging (#229) by @nikhars
- doc: Document how backpressure currently works (#227) by @untitaker
- ref: Remove extraneous codecs from arroyo (#219) by @untitaker

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
