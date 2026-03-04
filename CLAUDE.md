# CLAUDE.md — Streamline Kotlin SDK

## Overview

Kotlin client SDK for the Streamline streaming platform. Provides a coroutine-native client with WebSocket-based messaging, HTTP-based admin operations, Flow-based consumption, and pluggable telemetry.

## Build & Test

```bash
./gradlew build
./gradlew test
```

## Project Structure

```
src/main/kotlin/io/streamline/sdk/
  StreamlineClient.kt   # WebSocket client with reconnect, offline queue, Flow consumption
  AdminClient.kt        # HTTP REST admin client (topics, groups, queries, server info)
  Telemetry.kt          # Pluggable telemetry: Telemetry interface, ConsoleTelemetry, TracedClient, TracedAdminClient
  Models.kt             # Data classes, configuration, connection state, exceptions
src/test/kotlin/io/streamline/sdk/
  StreamlineClientTest.kt  # Models, configuration, exception hierarchy tests
  AdminClientTest.kt       # Admin operations with Ktor MockEngine
  TelemetryTest.kt         # Telemetry interface, console output, span lifecycle
```

## Architecture

- **StreamlineClient** connects via Ktor `HttpClient` with the `WebSockets` plugin over CIO engine for real-time messaging.
- **AdminClient** uses Ktor HTTP client with content negotiation for REST API calls to port 9094.
- **Flow consumption** via `callbackFlow` wraps subscription callbacks into `Flow<StreamlineMessage>`.
- **Telemetry** is pluggable via the `Telemetry` interface. `TracedClient` and `TracedAdminClient` wrap delegates with span instrumentation.
- Connection state is exposed as a `StateFlow<ConnectionState>`.
- Disconnected produce calls are buffered in a `Channel` (capacity 1000) and drained on reconnect.
- Reconnection uses exponential backoff via coroutine `delay`.
- All mutable shared state is protected by `Mutex`.

## Dependencies

- `kotlinx-coroutines-core` — structured concurrency
- `kotlinx-serialization-json` — JSON encoding/decoding
- `ktor-client-core` / `ktor-client-cio` / `ktor-client-websockets` — WebSocket transport
- `ktor-client-content-negotiation` / `ktor-serialization-kotlinx-json` — HTTP JSON support
- `ktor-client-mock` (test) — HTTP mock engine for admin client tests

## Conventions

- Model types are `@Serializable` data classes.
- All public API methods are `suspend` functions.
- Exceptions extend `StreamlineException` for a consistent error hierarchy.
- Admin operations throw typed exceptions (TopicNotFoundException, AuthenticationFailedException, AdminOperationException).
- Follows Kotlin coding conventions and kotlinx.coroutines best practices.
