# Changelog

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
