> ⚠️ **Community-Maintained SDK** — This SDK is in Alpha quality and maintained by the community. For production use, consider the [Java SDK](https://github.com/streamlinelabs/streamline-java-sdk) which provides full feature parity with Spring Boot integration. Contributions welcome!

# Streamline Kotlin SDK

[![CI](https://github.com/streamlinelabs/streamline-kotlin-sdk/actions/workflows/ci.yml/badge.svg)](https://github.com/streamlinelabs/streamline-kotlin-sdk/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Kotlin](https://img.shields.io/badge/Kotlin-2.0%2B-blue.svg)](https://kotlinlang.org/)
[![Docs](https://img.shields.io/badge/docs-streamlinelabs.dev-blue.svg)](https://streamlinelabs.dev/docs/sdks/kotlin)

Kotlin client SDK for [Streamline](https://github.com/streamlinelabs/streamline) — *The Redis of Streaming*.

## Requirements

- Kotlin 2.0+ / JDK 17+
- Streamline server 0.2.0 or later

## Installation

### Gradle (Kotlin DSL)

```kotlin
dependencies {
    implementation("io.streamline:streamline-kotlin-sdk:0.2.0")
}
```

### Maven

```xml
<dependency>
    <groupId>io.streamline</groupId>
    <artifactId>streamline-kotlin-sdk</artifactId>
    <version>0.2.0</version>
</dependency>
```

## Quick Start

```kotlin
import io.streamline.sdk.*

val client = StreamlineClient(
    StreamlineConfiguration(
        url = "ws://localhost:9092",
        authToken = "my-token",
    )
)

// Connect (suspending)
client.connect()

// Produce a message
client.produce("events", key = "user-1", value = """{"action":"click"}""")

// Subscribe to a topic
client.subscribe("events") { message ->
    println("Received: ${message.value}")
}

// Disconnect when done
client.disconnect()
client.close()
```

## Admin Client

The `AdminClient` communicates with the Streamline HTTP REST API (port 9094) for topic management, consumer group inspection, and SQL queries.

```kotlin
val admin = AdminClient("http://localhost:9094", authToken = "my-token")

// Topic management
admin.createTopic("events", partitions = 3)
val topics = admin.listTopics()
val details = admin.describeTopic("events")
admin.deleteTopic("old-topic")

// Consumer groups
val groups = admin.listConsumerGroups()
val groupDetails = admin.describeConsumerGroup("my-group")

// SQL queries
val result = admin.query("SELECT * FROM events LIMIT 10")
result.rows.forEach { row -> println(row) }

// Server info
val info = admin.serverInfo()
println("Version: ${info.version}, Topics: ${info.topicCount}")

admin.close()
```

## Flow-Based Consumption

Consume messages using Kotlin's `Flow` API for idiomatic streaming:

```kotlin
// Collect messages as a Flow
client.messages("events").collect { message ->
    println("Key: ${message.key}, Value: ${message.value}")
}

// Combine with Flow operators
client.messages("events")
    .filter { it.key != null }
    .take(100)
    .collect { println(it.value) }
```

## Telemetry

The SDK includes a pluggable telemetry API for distributed tracing. Use `ConsoleTelemetry` for development or implement the `Telemetry` interface for OpenTelemetry integration.

```kotlin
// Console telemetry (development)
val telemetry = ConsoleTelemetry("my-service")

// Traced message client
val traced = TracedClient(client, telemetry)
traced.produce("events", value = "hello")
traced.messages("events").collect { msg -> println(msg.value) }

// Traced admin client
val tracedAdmin = TracedAdminClient(admin, telemetry)
tracedAdmin.createTopic("events", partitions = 3)
tracedAdmin.query("SELECT count(*) FROM events")
```

## Features

- **Ktor WebSocket** connection to Streamline server
- **Coroutine-native** — all operations are `suspend` functions
- **Admin client** — topic CRUD, consumer groups, SQL queries via HTTP REST API
- **Flow-based consumption** — idiomatic `Flow<StreamlineMessage>` for streaming
- **Telemetry** — pluggable tracing with `ConsoleTelemetry` and W3C traceparent propagation
- **Auto-reconnect** with exponential backoff
- **Offline message queue** — messages produced while disconnected are buffered via `Channel`
- **StateFlow** for observable connection state
- **kotlinx.serialization** for type-safe message encoding

## Configuration

| Parameter | Default | Description |
|---|---|---|
| `url` | *(required)* | WebSocket URL of the Streamline server |
| `autoReconnect` | `true` | Automatically reconnect on disconnection |
| `maxRetries` | `10` | Maximum reconnection attempts |
| `timeoutMs` | `30000` | Connection timeout in milliseconds |
| `authToken` | `null` | Optional bearer token for authentication |
| `initialBackoffMs` | `500` | Initial reconnection backoff (ms) |
| `maxBackoffMs` | `30000` | Maximum backoff cap (ms) |

## Error Handling

```kotlin
try {
    client.produce("my-topic", "key", "value".toByteArray())
} catch (e: TopicNotFoundException) {
    println("Topic not found: ${e.message}")
    println("Hint: ${e.hint}")
} catch (e: StreamlineException) {
    if (e.retryable) {
        println("Retryable error: ${e.message}")
    } else {
        println("Fatal error: ${e.message}")
    }
}
```

## Contributing

Contributions are welcome! This is a community-maintained SDK. Please see the [organization contributing guide](https://github.com/streamlinelabs/.github/blob/main/CONTRIBUTING.md) for guidelines.

## License

Apache-2.0
