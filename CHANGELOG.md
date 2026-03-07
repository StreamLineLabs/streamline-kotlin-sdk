# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


## [Unreleased]

### Added
- `CircuitBreaker` class with CLOSED → OPEN → HALF_OPEN state machine for resilience
- `CircuitBreakerConfig` for configurable failure threshold, reset timeout, half-open probes
- `RetryPolicy` class with exponential backoff, jitter, and custom retry predicates
- `RetryPolicyConfig` for configurable max retries, base/max delay, jitter toggle
- `ErrorCode` enum with 15 machine-readable error categories
- `isRetryable` flag on all `StreamlineException` subclasses
- `hint` property on all exceptions with human-friendly resolution suggestions
- `CircuitBreakerOpenException` thrown when circuit is open
- `AuthorizationFailedException`, `ProducerException`, `ConsumerException`, `SerializationException` error types
- `circuitBreakerConfig` and `retryPolicyConfig` on `StreamlineConfiguration`
- CircuitBreaker + RetryPolicy integration in `StreamlineClient.sendWithRetry()`
- CircuitBreaker protection on `subscribe()` and `unsubscribe()` WebSocket sends
- Batch flush optimization — multi-message batches sent as single `produce_batch` frame
- Producer message batching with coroutine-based accumulator and configurable `batchSize`/`lingerMs`
- Producer retry logic with exponential backoff via `sendWithRetry` suspend function
- Compression type metadata included in WebSocket produce messages
- `producerConfig` property on `StreamlineClient` for runtime batching/retry configuration
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

