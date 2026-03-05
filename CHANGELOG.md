# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


- refactor: align DSL builder with Kotlin conventions (2026-03-05)

- feat: add coroutine-based consumer API (2026-03-05)
## [Unreleased]

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

