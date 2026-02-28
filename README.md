> ⚠️ **Community-Maintained SDK** — This SDK is in Alpha quality and maintained by the community. For production use, consider the [Java SDK](https://github.com/streamlinelabs/streamline-java-sdk) which provides full feature parity with Spring Boot integration. Contributions welcome!

# Streamline Kotlin SDK

Kotlin client SDK for [Streamline](https://github.com/streamlinelabs/streamline) — *The Redis of Streaming*.

## Requirements

- Kotlin 2.0+ / JDK 17+

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

## Features

- **Ktor WebSocket** connection to Streamline server
- **Coroutine-native** — all operations are `suspend` functions
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

## License

Apache-2.0
