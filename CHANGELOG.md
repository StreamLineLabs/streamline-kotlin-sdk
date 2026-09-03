# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


## [Unreleased]

### Security
- Bind local Ed25519 attestation verification to the consumed payload hash,
  topic, partition, and offset.
- Bind attestation `key_id` to a trusted expected key ID or a caller-supplied
  `TrustedKeyResolver`, so identity is authenticated against a specific
  registered key instead of a self-declared, unauthenticated label. The
  original single-`PublicKey` constructor remains source-compatible but is
  deprecated and now fails verification closed because no trusted key ID was
  supplied.
- Reject unwired SCRAM, legacy `SaslConfig`, and client-certificate/mTLS
  settings instead of silently accepting them.
- Redact bearer tokens, passwords, and credential providers from public
  configuration `toString()` output.
- `ProducerConfig.acks` now defaults to `Acks.NONE` (previously `Acks.ONE`),
  and `ProducerConfig.validate()` rejects `Acks.ONE`/`Acks.ALL` and
  `idempotent = true` with `ConfigurationException`, enforced both when
  `producerConfig` is assigned and again at every `produce`/`flushBatch`/
  `produceBatch` send. The WebSocket produce command has no correlated
  per-message broker acknowledgment, so honoring those settings would
  silently claim a delivery/deduplication guarantee the transport cannot
  verify; only `Acks.NONE` with a non-idempotent producer is honored
  end-to-end. `Acks.ONE`/`Acks.ALL` and `idempotent` remain on the type only
  for source compatibility.


## [0.3.0] - 2026-04-20

### Added
- `io.streamline.sdk.moonshot` package — Ktor + coroutines clients for the
  Streamline Moonshot HTTP control plane (port `9094`): `BranchesClient`,
  `ContractsClient`, `AttestationClient`, `SearchClient`, `MemoryClient`.
- Shared `MoonshotOptions`, `MoonshotException`, and serializable DTOs
  (`Branch`, `MergeReport`, `ContractValidationResult`, `Attestation`,
  `SearchHit`, `MemoryRecord`, `MemoryKind`).

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
- test: verify consumer flow cancellation and backpressure
- chore: bump Kotlin Coroutines dependency to 1.9.0
