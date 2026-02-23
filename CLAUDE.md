# CLAUDE.md — Streamline Kotlin SDK

## Overview

Kotlin client SDK for the Streamline streaming platform. Provides a coroutine-native, Ktor WebSocket-based client for producing and consuming messages with auto-reconnect and offline buffering.

## Build & Test

```bash
./gradlew build
./gradlew test
```

## Project Structure

```
src/main/kotlin/io/streamline/sdk/
  StreamlineClient.kt   # Main client with Ktor WebSocket, reconnect, offline queue
  Models.kt             # Data classes, configuration, connection state, exceptions
src/test/kotlin/io/streamline/sdk/
  StreamlineClientTest.kt
```

## Architecture

- **StreamlineClient** connects via Ktor `HttpClient` with the `WebSockets` plugin over CIO engine.
- Connection state is exposed as a `StateFlow<ConnectionState>`.
- Disconnected produce calls are buffered in a `Channel` (capacity 1000) and drained on reconnect.
- Reconnection uses exponential backoff via coroutine `delay`.
- All mutable shared state is protected by `Mutex`.

## Dependencies

- `kotlinx-coroutines-core` — structured concurrency
- `kotlinx-serialization-json` — JSON encoding/decoding
- `ktor-client-core` / `ktor-client-cio` / `ktor-client-websockets` — WebSocket transport

## Conventions

- Model types are `@Serializable` data classes.
- All public API methods are `suspend` functions.
- Exceptions extend `StreamlineException` for a consistent error hierarchy.
- Follows Kotlin coding conventions and kotlinx.coroutines best practices.
