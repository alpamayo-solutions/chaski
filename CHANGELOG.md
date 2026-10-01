# Changelog

## [0.20.0](https://github.com/alpamayo-solutions/chaski/compare/v0.19.3...v0.20.0) (2026-10-01)


### Features

* export Backoff, Command helpers, run_connector and save_checkpoint ([#113](https://github.com/alpamayo-solutions/chaski/issues/113)) ([294de9c](https://github.com/alpamayo-solutions/chaski/commit/294de9ccd2c371ea0db4d0df6682c6a0cfe0d85b))

## [0.19.3](https://github.com/alpamayo-solutions/chaski/compare/v0.19.2...v0.19.3) (2026-10-01)


### Fixes

* **deps:** accept colca 0.26 ([#111](https://github.com/alpamayo-solutions/chaski/issues/111)) ([0945952](https://github.com/alpamayo-solutions/chaski/commit/094595223efcd5ee78b7c4b25d4fdbf0641f45f5))

## [0.19.2](https://github.com/alpamayo-solutions/chaski/compare/v0.19.1...v0.19.2) (2026-10-01)


### Fixes

* **command:** never let a command expire after the write deadline it was sent under ([#109](https://github.com/alpamayo-solutions/chaski/issues/109)) ([a3ac6d0](https://github.com/alpamayo-solutions/chaski/commit/a3ac6d03b5a9dca3301977d6a6ec6284d6400811))

## [0.19.1](https://github.com/alpamayo-solutions/chaski/compare/v0.19.0...v0.19.1) (2026-10-01)


### Fixes

* **consume:** end a consumer's wait when stop is set, without a ring ([#107](https://github.com/alpamayo-solutions/chaski/issues/107)) ([f544b92](https://github.com/alpamayo-solutions/chaski/commit/f544b9211ebbd1474d80b0a6363413ede7f7f02e))
* **wakeup:** ring wake_on filters with + and # for every matching topic ([#106](https://github.com/alpamayo-solutions/chaski/issues/106)) ([ceef5cf](https://github.com/alpamayo-solutions/chaski/commit/ceef5cfdba66c3dd4a65854cd998f8b711694441))

## [0.19.0](https://github.com/alpamayo-solutions/chaski/compare/v0.18.0...v0.19.0) (2026-10-01)


### ⚠ BREAKING CHANGES

* **command:** Driver.write(target, value) is now Driver.write(target, value, command). Drivers that write add the argument.

### Features

* **command:** carry the person a command acts for and make operations idempotent ([#104](https://github.com/alpamayo-solutions/chaski/issues/104)) ([67fb6b4](https://github.com/alpamayo-solutions/chaski/commit/67fb6b48ff0ce79daf354a05ce6c4d77632abfa3))

## [0.18.0](https://github.com/alpamayo-solutions/chaski/compare/v0.17.1...v0.18.0) (2026-10-01)


### Features

* **retained-view:** expose applied stream positions for read-after-write ([#102](https://github.com/alpamayo-solutions/chaski/issues/102)) ([9e9f6da](https://github.com/alpamayo-solutions/chaski/commit/9e9f6dab429cb965632310b980cd8172c8a6924f))

## [0.17.1](https://github.com/alpamayo-solutions/chaski/compare/v0.17.0...v0.17.1) (2026-09-30)


### Fixes

* **deps:** accept colca 0.25 ([#100](https://github.com/alpamayo-solutions/chaski/issues/100)) ([9a017ce](https://github.com/alpamayo-solutions/chaski/commit/9a017ceaac4bb5881e4f30dc339a75761803dd67))

## [0.17.0](https://github.com/alpamayo-solutions/chaski/compare/v0.16.1...v0.17.0) (2026-09-30)


### ⚠ BREAKING CHANGES

* **command:** Service.command, CommandSender.command and Runtime.command require lifetime=. Pass the old timeout to keep the old expiry, or None for a command that waits for its node. chaski.lifetime_refusal and chaski.command.MAX_LIFETIME_S are removed.

### Features

* **command:** command a node below, choose the lifetime, wait separately ([#97](https://github.com/alpamayo-solutions/chaski/issues/97)) ([e9d647c](https://github.com/alpamayo-solutions/chaski/commit/e9d647c8a48fa301a5f0ee77c77e486ef74574b4))
* **connector:** write a signal through the connector that binds it ([#99](https://github.com/alpamayo-solutions/chaski/issues/99)) ([b1a6f09](https://github.com/alpamayo-solutions/chaski/commit/b1a6f0921e717e2e5c74cb4cb9ac06d031efadf3))

## [0.16.1](https://github.com/alpamayo-solutions/chaski/compare/v0.16.0...v0.16.1) (2026-09-30)


### Fixes

* **deps:** accept colca 0.23 ([#95](https://github.com/alpamayo-solutions/chaski/issues/95)) ([5004545](https://github.com/alpamayo-solutions/chaski/commit/5004545c747f41bd28776e12ed8c5e38052161a6))

## [0.16.0](https://github.com/alpamayo-solutions/chaski/compare/v0.15.1...v0.16.0) (2026-09-30)


### ⚠ BREAKING CHANGES

* **retained-view:** retained_view() and RetainedView require scope=. Pass chaski.ViewScope([...]) with the paths the view needs, or ViewScope.whole_node() for the previous behaviour.

### Fixes

* **retained-view:** read only the paths a view is scoped to ([#93](https://github.com/alpamayo-solutions/chaski/issues/93)) ([4afe03b](https://github.com/alpamayo-solutions/chaski/commit/4afe03bc5869c0065eab41fdeb68193d431f3346))

## [0.15.1](https://github.com/alpamayo-solutions/chaski/compare/v0.15.0...v0.15.1) (2026-09-28)


### Fixes

* **deps:** accept colca 0.22 ([#91](https://github.com/alpamayo-solutions/chaski/issues/91)) ([541954f](https://github.com/alpamayo-solutions/chaski/commit/541954fd48cbee089a866b65912a2202b5be91f2))

## [0.15.0](https://github.com/alpamayo-solutions/chaski/compare/v0.14.5...v0.15.0) (2026-09-28)


### Features

* **service:** scoped push wake-ups, view recovery, Retry-After backoff and identity conflicts ([#88](https://github.com/alpamayo-solutions/chaski/issues/88)) ([27d2086](https://github.com/alpamayo-solutions/chaski/commit/27d2086c446fae40a7b691e37a9452f1cb9fbfd0))

## [0.14.5](https://github.com/alpamayo-solutions/chaski/compare/v0.14.4...v0.14.5) (2026-09-27)


### Fixes

* command executor wakes on its own answers; checkpoint save and handler command deadline fixes ([#86](https://github.com/alpamayo-solutions/chaski/issues/86)) ([98f0bd4](https://github.com/alpamayo-solutions/chaski/commit/98f0bd4a64f10db853c96258336debcabd5bdbbd))

## [0.14.4](https://github.com/alpamayo-solutions/chaski/compare/v0.14.3...v0.14.4) (2026-09-27)


### Fixes

* **connector:** wake acquisition at scheduled factory-clock starts ([#84](https://github.com/alpamayo-solutions/chaski/issues/84)) ([04143bf](https://github.com/alpamayo-solutions/chaski/commit/04143bff19d76cfccf58a5af583b1b3d887caf06))

## [0.14.3](https://github.com/alpamayo-solutions/chaski/compare/v0.14.2...v0.14.3) (2026-09-27)


### Fixes

* a command runs only while the executor's broker link is up, and its writes are not queued past its deadline ([#82](https://github.com/alpamayo-solutions/chaski/issues/82)) ([a37774a](https://github.com/alpamayo-solutions/chaski/commit/a37774ae0f1418d6baacb0a673134c1880745027))

## [0.14.2](https://github.com/alpamayo-solutions/chaski/compare/v0.14.1...v0.14.2) (2026-09-27)


### Fixes

* a record rejected during the code-change replay is recorded and passed, and a failed replay is retried with the service up ([#79](https://github.com/alpamayo-solutions/chaski/issues/79)) ([7a8ee7a](https://github.com/alpamayo-solutions/chaski/commit/7a8ee7a0ef169e334f5a5fc3c3ea4426a456951d))
* a retained view fetches with its contracts on every drain, so a cursor at the head does not keep an unfiltered read ([#78](https://github.com/alpamayo-solutions/chaski/issues/78)) ([91dbde0](https://github.com/alpamayo-solutions/chaski/commit/91dbde0578927534cbe34d30d094bde631c716a6))

## [0.14.1](https://github.com/alpamayo-solutions/chaski/compare/v0.14.0...v0.14.1) (2026-09-27)


### Fixes

* retained view and command executor fetch only what wakes them, so other records do not count as unread ([#76](https://github.com/alpamayo-solutions/chaski/issues/76)) ([3270675](https://github.com/alpamayo-solutions/chaski/commit/32706759ccedf8e737afefbdb5e87aae709c12f3))

## [0.14.0](https://github.com/alpamayo-solutions/chaski/compare/v0.13.0...v0.14.0) (2026-09-27)


### Features

* **dataops:** keep the command executor alive through broker outages and answer unconfirmed writes 504 ([#74](https://github.com/alpamayo-solutions/chaski/issues/74)) ([3ce98e1](https://github.com/alpamayo-solutions/chaski/commit/3ce98e17617367c49e9eb92055b2fd6caad3922d))

## [0.13.0](https://github.com/alpamayo-solutions/chaski/compare/v0.12.0...v0.13.0) (2026-09-27)


### ⚠ BREAKING CHANGES

* a handler that raises on input it can never process now blocks its stream (retried with backoff, service unhealthy after five failures) instead of being skipped. Raise chaski.Reject for such input; the rejection is recorded as the service's rejected_input _Finding before the ack. Handlers must be idempotent.
* enforce durable push-driven consumer recovery

### Features

* enforce durable push-driven consumer recovery ([c30a0c0](https://github.com/alpamayo-solutions/chaski/commit/c30a0c0dc25c52d81e34913057586a4ab62da010))
* retry failed handlers instead of skipping them; Reject sets input aside durably ([#72](https://github.com/alpamayo-solutions/chaski/issues/72)) ([bd50688](https://github.com/alpamayo-solutions/chaski/commit/bd50688710949bed11ae0af8fb13c60c6419d8e9))


### Documentation

* publish push-driven usage rules and broker compatibility ([e13faa2](https://github.com/alpamayo-solutions/chaski/commit/e13faa21f129db12e347e906bfc4faffadbc3b51))

## [0.12.0](https://github.com/alpamayo-solutions/chaski/compare/v0.11.1...v0.12.0) (2026-09-26)


### Features

* consumers read when woken, never on a timer, and report the node's cursor_lag ([#67](https://github.com/alpamayo-solutions/chaski/issues/67)) ([899bff5](https://github.com/alpamayo-solutions/chaski/commit/899bff524962de5d1ed47e95f7e20fcf015a2089))


### Fixes

* move cursors past the records a filtered page skipped; commands follow the stream ([#68](https://github.com/alpamayo-solutions/chaski/issues/68)) ([657ce9b](https://github.com/alpamayo-solutions/chaski/commit/657ce9b0fbeb2dbc2cdc4b324ad3746a900479ea))

## [0.11.1](https://github.com/alpamayo-solutions/chaski/compare/v0.11.0...v0.11.1) (2026-09-26)


### Performance

* **dataops:** paced ingest, read-ahead when behind, one buffer commit per page ([#65](https://github.com/alpamayo-solutions/chaski/issues/65)) ([29c1707](https://github.com/alpamayo-solutions/chaski/commit/29c170775f163753f7248b5008999e610e08c5b0))

## [0.11.0](https://github.com/alpamayo-solutions/chaski/compare/v0.10.11...v0.11.0) (2026-09-26)


### Features

* announce the commands a service executes in _ServiceDetails ([bbb50b2](https://github.com/alpamayo-solutions/chaski/commit/bbb50b26871924827b6de57a29dac9f023ae2493))
* **dataops:** a live resolution index instead of a KV read per burst ([bbb50b2](https://github.com/alpamayo-solutions/chaski/commit/bbb50b26871924827b6de57a29dac9f023ae2493))
* **door:** watch stream hints, fetch by contract, kv by depth ([bbb50b2](https://github.com/alpamayo-solutions/chaski/commit/bbb50b26871924827b6de57a29dac9f023ae2493))

## [0.10.11](https://github.com/alpamayo-solutions/chaski/compare/v0.10.10...v0.10.11) (2026-09-26)


### Fixes

* **dataops:** no full KV read per command; a busy node answers 503 ([#61](https://github.com/alpamayo-solutions/chaski/issues/61)) ([d17c965](https://github.com/alpamayo-solutions/chaski/commit/d17c96594f88f3dcea501a3658724341153a5464))

## [0.10.10](https://github.com/alpamayo-solutions/chaski/compare/v0.10.9...v0.10.10) (2026-09-26)


### Fixes

* keep the MQTT session alive through a misframed packet and a broker restart ([#59](https://github.com/alpamayo-solutions/chaski/issues/59)) ([cd34e74](https://github.com/alpamayo-solutions/chaski/commit/cd34e74d4c33246b5cd2445b3ebf1b1ba4add7ad))

## [0.10.9](https://github.com/alpamayo-solutions/chaski/compare/v0.10.8...v0.10.9) (2026-09-26)


### Fixes

* **dataops:** run [@on](https://github.com/on)_metric handlers on the service's event loop ([#57](https://github.com/alpamayo-solutions/chaski/issues/57)) ([84bc429](https://github.com/alpamayo-solutions/chaski/commit/84bc42965f88919408c2ebf1483f979fd0254add))

## [0.10.8](https://github.com/alpamayo-solutions/chaski/compare/v0.10.7...v0.10.8) (2026-09-25)


### Fixes

* **service:** re-announce when its own record reads inactive ([#55](https://github.com/alpamayo-solutions/chaski/issues/55)) ([ad7e8f0](https://github.com/alpamayo-solutions/chaski/commit/ad7e8f0eafcf825d8eb54c7d550237ed416b2a54))

## [0.10.7](https://github.com/alpamayo-solutions/chaski/compare/v0.10.6...v0.10.7) (2026-09-25)


### Fixes

* **deps:** allow Colca 0.17 releases ([#53](https://github.com/alpamayo-solutions/chaski/issues/53)) ([97e943a](https://github.com/alpamayo-solutions/chaski/commit/97e943add409773f80f29302b72956c494fdce12))

## [0.10.6](https://github.com/alpamayo-solutions/chaski/compare/v0.10.5...v0.10.6) (2026-09-25)


### Fixes

* **deps:** allow Colca 0.15 releases ([#51](https://github.com/alpamayo-solutions/chaski/issues/51)) ([75a5524](https://github.com/alpamayo-solutions/chaski/commit/75a55240b7f86fe386eaea33cd5a6d4d0c88d4bc))

## [0.10.5](https://github.com/alpamayo-solutions/chaski/compare/v0.10.4...v0.10.5) (2026-09-25)


### Fixes

* **catalogue:** normalize retained boolean data types ([#19](https://github.com/alpamayo-solutions/chaski/issues/19)) ([124b703](https://github.com/alpamayo-solutions/chaski/commit/124b7035f66a5c5ea939b70822acf22a8f8efee0))
* **deps:** allow compatible Colca 0.14 clock releases ([#50](https://github.com/alpamayo-solutions/chaski/issues/50)) ([08e9e61](https://github.com/alpamayo-solutions/chaski/commit/08e9e6118a67372083e00ddba241f9ee7281a045))

## [0.10.4](https://github.com/alpamayo-solutions/chaski/compare/v0.10.3...v0.10.4) (2026-09-25)


### Fixes

* **dataops:** cap a command's lifetime on the receiving side ([#47](https://github.com/alpamayo-solutions/chaski/issues/47)) ([57eaf3c](https://github.com/alpamayo-solutions/chaski/commit/57eaf3c9a0a4a4deb6ea77e0fc35197cd788bda5))

## [0.10.3](https://github.com/alpamayo-solutions/chaski/compare/v0.10.2...v0.10.3) (2026-09-25)


### Fixes

* **service:** an undecodable message is a warning again ([#45](https://github.com/alpamayo-solutions/chaski/issues/45)) ([4ae1bb7](https://github.com/alpamayo-solutions/chaski/commit/4ae1bb7be4663e034419224957fa2052dc3bae50))

## [0.10.2](https://github.com/alpamayo-solutions/chaski/compare/v0.10.1...v0.10.2) (2026-09-24)


### Fixes

* **deps:** franzmq 0.6.5 decodes the node's commands and acks ([#43](https://github.com/alpamayo-solutions/chaski/issues/43)) ([be5a28c](https://github.com/alpamayo-solutions/chaski/commit/be5a28cd557443947344b6f08144fc6e1d1efd07))

## [0.10.1](https://github.com/alpamayo-solutions/chaski/compare/v0.10.0...v0.10.1) (2026-09-24)


### Fixes

* **clock:** require ordered sample completion before advancing windows ([#41](https://github.com/alpamayo-solutions/chaski/issues/41)) ([2d7819f](https://github.com/alpamayo-solutions/chaski/commit/2d7819f825b1c33f7c72df909362f1c89c75751c))

## [0.10.0](https://github.com/alpamayo-solutions/chaski/compare/v0.9.0...v0.10.0) (2026-09-24)


### Features

* **clock:** add optional application time and coordinated execution windows ([#39](https://github.com/alpamayo-solutions/chaski/issues/39)) ([821abfe](https://github.com/alpamayo-solutions/chaski/commit/821abfeee8c6ae084f998aa82cfeeae11e54c1d1))

## [0.9.0](https://github.com/alpamayo-solutions/chaski/compare/v0.8.1...v0.9.0) (2026-09-24)


### Features

* write records and send commands over the MQTT session ([#35](https://github.com/alpamayo-solutions/chaski/issues/35)) ([74da8a0](https://github.com/alpamayo-solutions/chaski/commit/74da8a0c45a0ac68f5f184311bfaef1b9ef4446f))


### Fixes

* allow colca-data-contracts up to 0.11 ([#36](https://github.com/alpamayo-solutions/chaski/issues/36)) ([f7c2321](https://github.com/alpamayo-solutions/chaski/commit/f7c2321fbacbe540b5ee509c089a1de5ebd3613f))
* **dataops:** the health door answers 503 when ingest stops finishing drains ([#37](https://github.com/alpamayo-solutions/chaski/issues/37)) ([767c1ae](https://github.com/alpamayo-solutions/chaski/commit/767c1aeb35bbf8f83e8141e6edbbf9f5f9ab1f38))


### Performance

* **dataops:** lazy pandas import and one KV read per watch burst ([#34](https://github.com/alpamayo-solutions/chaski/issues/34)) ([1bfec28](https://github.com/alpamayo-solutions/chaski/commit/1bfec284408baf11f88a49a9b1c2f6be072f777e))

## [0.8.1](https://github.com/alpamayo-solutions/chaski/compare/v0.8.0...v0.8.1) (2026-09-24)


### Fixes

* tolerate undecodable messages from the moment the client connects ([#31](https://github.com/alpamayo-solutions/chaski/issues/31)) ([5a05e27](https://github.com/alpamayo-solutions/chaski/commit/5a05e276d3bd61be9b79d7a78e1aa3262c28d52e))

## [0.8.0](https://github.com/alpamayo-solutions/chaski/compare/v0.7.0...v0.8.0) (2026-09-24)


### Features

* **dataops:** SignalOutput carries unit, semantic_type and description ([e6710fa](https://github.com/alpamayo-solutions/chaski/commit/e6710fa9e26d1174280d2ef40a5dc7be03e81a6a))


### Fixes

* **dataops:** keep output tag ids across restarts ([e6710fa](https://github.com/alpamayo-solutions/chaski/commit/e6710fa9e26d1174280d2ef40a5dc7be03e81a6a))

## [0.7.0](https://github.com/alpamayo-solutions/chaski/compare/v0.6.1...v0.7.0) (2026-09-23)


### Features

* **dataops:** execute commands from a producer ([@on](https://github.com/on)_command) ([#26](https://github.com/alpamayo-solutions/chaski/issues/26)) ([5c50765](https://github.com/alpamayo-solutions/chaski/commit/5c5076554508c98737bad7f27220fbd72ad692e4))

## [0.6.1](https://github.com/alpamayo-solutions/chaski/compare/v0.6.0...v0.6.1) (2026-09-23)


### Fixes

* **dataops:** keep the service up when the wake-topic read fails ([#25](https://github.com/alpamayo-solutions/chaski/issues/25)) ([cf014f8](https://github.com/alpamayo-solutions/chaski/commit/cf014f8ad8511583d754f2be07f02d2c6deae127))

## [0.6.0](https://github.com/alpamayo-solutions/chaski/compare/v0.5.1...v0.6.0) (2026-09-23)


### Features

* **dataops:** send an annotation's element and related annotations ([#23](https://github.com/alpamayo-solutions/chaski/issues/23)) ([655ac31](https://github.com/alpamayo-solutions/chaski/commit/655ac317212072365b34f75a6ba86966d57505de))

## [0.5.1](https://github.com/alpamayo-solutions/chaski/compare/v0.5.0...v0.5.1) (2026-09-23)


### Fixes

* **dataops:** wake the ingest only on the service's own input topics ([#21](https://github.com/alpamayo-solutions/chaski/issues/21)) ([2922ea5](https://github.com/alpamayo-solutions/chaski/commit/2922ea5bef49b8f4be18b42c756a1a1e14925cb8))

## [0.5.0](https://github.com/alpamayo-solutions/chaski/compare/v0.4.0...v0.5.0) (2026-09-23)


### Features

* **door:** retire a retained record with a tombstone ([857b564](https://github.com/alpamayo-solutions/chaski/commit/857b564f18330577953980563b7132ed4bafcce7))

## [0.4.0](https://github.com/alpamayo-solutions/chaski/compare/v0.3.1...v0.4.0) (2026-09-20)


### Features

* **dataops:** delete one annotation by the identity write_interval gave it ([#16](https://github.com/alpamayo-solutions/chaski/issues/16)) ([56171e0](https://github.com/alpamayo-solutions/chaski/commit/56171e02f31822a0ec4e40a48dee9dfc1194fceb))
* **dataops:** update or delete an annotation by the id its write returned ([#18](https://github.com/alpamayo-solutions/chaski/issues/18)) ([f9dcc32](https://github.com/alpamayo-solutions/chaski/commit/f9dcc32dd32bf224c8f1fdd78c0dbe7c6fdaf549))

## [0.3.1](https://github.com/alpamayo-solutions/chaski/compare/v0.3.0...v0.3.1) (2026-09-19)


### Fixes

* **deps:** ship the colca 0.6 support in a release ([#14](https://github.com/alpamayo-solutions/chaski/issues/14)) ([46caa39](https://github.com/alpamayo-solutions/chaski/commit/46caa39ca33172d9da2bf2923f73c8a62722e549))

## [0.3.0](https://github.com/alpamayo-solutions/chaski/compare/v0.2.1...v0.3.0) (2026-09-19)


### Features

* **dataops:** on_constant/on_signal triggers and an on_ready hook ([#11](https://github.com/alpamayo-solutions/chaski/issues/11)) ([d8d0db6](https://github.com/alpamayo-solutions/chaski/commit/d8d0db67eb119074f8dca05b97683c3691d8cac3))

## [0.2.1](https://github.com/alpamayo-solutions/chaski/compare/v0.2.0...v0.2.1) (2026-09-19)


### Fixes

* a retired binding reaches the service instead of killing it ([#9](https://github.com/alpamayo-solutions/chaski/issues/9)) ([2599890](https://github.com/alpamayo-solutions/chaski/commit/2599890711df8b1174b5ca402ed296573574edf3))

## [0.2.0](https://github.com/alpamayo-solutions/chaski/compare/v0.1.2...v0.2.0) (2026-09-18)


### ⚠ BREAKING CHANGES

* run against colca 0.3 ([#6](https://github.com/alpamayo-solutions/chaski/issues/6))

### Features

* run against colca 0.3 ([#6](https://github.com/alpamayo-solutions/chaski/issues/6)) ([5c74a07](https://github.com/alpamayo-solutions/chaski/commit/5c74a07346fa580969348abd196f0f82b86118f2))

## [0.1.2](https://github.com/alpamayo-solutions/chaski/compare/v0.1.1...v0.1.2) (2026-09-12)


### Fixes

* **dataops:** discover producers built with type() ([4ac98ad](https://github.com/alpamayo-solutions/chaski/commit/4ac98ada6baa86592a2c93e7227cb19cd34158cd))

## [0.1.1](https://github.com/alpamayo-solutions/chaski/compare/v0.1.0...v0.1.1) (2026-09-11)


### Fixes

* **package:** bound the colca dependencies to their 0.1 releases ([bc35d3a](https://github.com/alpamayo-solutions/chaski/commit/bc35d3ac118f83236a12b6f048307ab9e129d939))


### Documentation

* install from PyPI ([98b864c](https://github.com/alpamayo-solutions/chaski/commit/98b864ccd49cf0c7c27948ec2c79177ed3a0d487))

## 0.1.0 (2026-09-11)

First public release. See the [README](https://github.com/alpamayo-solutions/chaski#readme) for what chaski does and how to install it.
