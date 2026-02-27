# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0] - 2026-02-18

### Added
- Kotlin coroutine-native client with suspend functions
- WebSocket-based transport layer via Ktor
- Auto-reconnect with exponential backoff
- Offline message queue for connection interruptions
- StateFlow-based connection state observation
- kotlinx-serialization for message encoding
- JUnit 5 test suite

### Infrastructure
- Gradle build with Kotlin 2.0
- Maven publish plugin configuration
- Apache 2.0 license
