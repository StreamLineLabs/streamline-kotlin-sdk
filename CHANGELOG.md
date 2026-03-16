# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


## [Unreleased]

### Added
- Producer message batching with coroutine-based accumulator and configurable `batchSize`/`lingerMs`
- Producer retry logic with exponential backoff via `sendWithRetry` suspend function
- Compression type metadata included in WebSocket produce messages
- `producerConfig` property on `StreamlineClient` for runtime batching/retry configuration
- Circuit breaker pattern (`CircuitBreaker`) with configurable failure/success thresholds
- `ErrorCode` enum with `retryable` flag and `hint` on all exception types
- Consumer offset management: `commitOffsets`, `seekToOffset`, `seekToBeginning`, `seekToEnd`, `position`, `committed`
- AdminClient: cluster info via `clusterInfo()` and `listBrokers()`
- AdminClient: consumer group lag monitoring via `consumerGroupLag()` and `consumerGroupTopicLag()`
- AdminClient: offset reset via `resetOffsets()` and `resetOffsetsDryRun()`
- AdminClient: message inspection via `inspectMessages()` and `latestMessages()`
- AdminClient: server metrics via `metricsHistory()`
- Model types: `ClusterInfo`, `BrokerInfo`, `ConsumerLag`, `ConsumerGroupLag`, `InspectedMessage`, `MetricPoint`
- Tests for all new AdminClient methods (mock-engine based)
- Expanded error handling documentation in README with all 9 exception types
- CODEOWNERS file for review assignment

### Changed
- refactor: align DSL builder with Kotlin conventions (2026-03-05)
- feat: add coroutine-based consumer API (2026-03-05)
- fix: resolve suspend function cancellation handling (2026-03-06)
- test: add flow-based consumption tests (2026-03-06)

## [0.2.0] - 2026-02-28

### Added
- Kotlin coroutine-native client with suspend functions
- WebSocket-based transport layer via Ktor
- Auto-reconnect with exponential backoff
- Offline message queue for connection interruptions
- StateFlow-based connection state observation
- kotlinx-serialization for message encoding
- Batch message support
- Serialization support for Avro schemas
- Flow-based message consumption
- JUnit 5 test suite
- Integration tests for producer

### Fixed
- Correct serialization for headers
- Resolve coroutine scope cancellation

### Changed
- Extract client configuration
- Extract data models to separate file

### Infrastructure
- Gradle build with Kotlin 2.0
- Maven publish plugin configuration
- Apply ktlint formatting rules
- Apache 2.0 license

- feat: implement Flow-based consumer API for Coroutines
- docs: update Kotlin Coroutines streaming integration guide
- fix: resolve coroutine scope leak on consumer close
