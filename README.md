> ⚠️ **Community-Maintained SDK** — This SDK is in Alpha quality and maintained by the community. For production use, consider the [Java SDK](https://github.com/streamlinelabs/streamline-java-sdk) which provides full feature parity with Spring Boot integration. Contributions welcome!

# Streamline Kotlin SDK

[![CI](https://github.com/streamlinelabs/streamline-kotlin-sdk/actions/workflows/ci.yml/badge.svg)](https://github.com/streamlinelabs/streamline-kotlin-sdk/actions/workflows/ci.yml)
[![codecov](https://img.shields.io/codecov/c/github/streamlinelabs/streamline-kotlin-sdk?style=flat-square)](https://codecov.io/gh/streamlinelabs/streamline-kotlin-sdk)
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

## Schema Registry

The SDK includes a full Schema Registry client compatible with the Confluent wire format:

```kotlin
val registry = SchemaRegistryClient("http://localhost:9094")

// Register a schema
val id = registry.registerSchema("events-value", avroSchemaJson, SchemaFormat.AVRO)

// Retrieve the latest schema
val schema = registry.getLatestSchema("events-value")
println("Version: ${schema.version}, Type: ${schema.schemaType}")

// Check compatibility before evolving
val compatible = registry.checkCompatibility("events-value", newSchema)

// List subjects and versions
val subjects = registry.listSubjects()
val versions = registry.listVersions("events-value")

registry.close()
```

Supports **AVRO**, **PROTOBUF**, and **JSON** schema formats.

## Security

### TLS

```kotlin
val config = StreamlineConfiguration(
    url = "wss://streamline.example.com:9092",
    tls = TlsConfig(
        enabled = true,
        trustStorePath = "/etc/ssl/truststore.jks",
        trustStorePassword = "changeit",
    ),
)
```

### SASL Authentication

```kotlin
val config = StreamlineConfiguration(
    url = "ws://streamline.example.com:9092",
    sasl = SaslConfig(
        mechanism = SaslMechanism.SCRAM_SHA_256,
        username = "admin",
        password = "secret",
    ),
)
```

## Producer & Consumer Configuration

```kotlin
// Producer tuning
val producerConfig = ProducerConfig(
    batchSize = 32768,
    lingerMs = 5,
    compression = CompressionType.LZ4,
    acks = Acks.ALL,
    idempotent = true,
)

// Consumer tuning
val consumerConfig = ConsumerConfig(
    groupId = "my-app",
    autoCommit = false,
    maxPollRecords = 1000,
    autoOffsetReset = OffsetReset.EARLIEST,
)
```

## Features

- **Ktor WebSocket** connection to Streamline server
- **Coroutine-native** — all operations are `suspend` functions
- **Admin client** — topic CRUD, consumer groups, SQL queries via HTTP REST API
- **Schema Registry** — register, retrieve, and validate schemas (Avro, Protobuf, JSON)
- **Security** — TLS encryption and SASL authentication (PLAIN, SCRAM-SHA-256/512)
- **Producer/Consumer config** — batching, compression, acknowledgments, consumer groups
- **Flow-based consumption** — idiomatic `Flow<StreamlineMessage>` for streaming
- **Circuit breaker** — CLOSED → OPEN → HALF_OPEN state machine protects against cascading failures
- **Retry policy** — exponential backoff with jitter for transient errors; integrates with `isRetryable` error classification
- **Structured error handling** — `ErrorCode` enum, `isRetryable` flag, and `hint` on every exception
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
| `tls` | `null` | TLS configuration (see [Security](#security)) |
| `sasl` | `null` | SASL authentication (see [Security](#security)) |

## Error Handling

All SDK exceptions extend `StreamlineException` and include a machine-readable `code` (`ErrorCode` enum), an `isRetryable` flag, and a human-friendly `hint`:

| Exception | Code | Retryable? | Hint |
|-----------|------|------------|------|
| `NotConnectedException` | `CONNECTION` | Yes | Call connect() first |
| `ConnectionFailedException` | `CONNECTION` | Yes | Check server URL |
| `AuthenticationFailedException` | `AUTHENTICATION` | No | Verify credentials |
| `AuthorizationFailedException` | `AUTHORIZATION` | No | Check ACL permissions |
| `StreamlineTimeoutException` | `TIMEOUT` | Yes | Increase timeout |
| `TopicNotFoundException` | `TOPIC_NOT_FOUND` | No | Create topic first |
| `OfflineQueueFullException` | `OFFLINE_QUEUE_FULL` | No | Reduce send rate |
| `CircuitBreakerOpenException` | `CIRCUIT_BREAKER_OPEN` | Yes | Retry after reset timeout |
| `ProducerException` | `PRODUCER` | Yes | Check message size |
| `ConsumerException` | `CONSUMER` | Yes | Check group config |
| `SerializationException` | `SERIALIZATION` | No | Verify message format |
| `AdminOperationException` | `ADMIN` | Yes | Check connectivity |
| `QueryException` | `QUERY` | No | Verify SQL syntax |
| `SchemaRegistryException` | `SCHEMA_REGISTRY` | Yes | Check registry connectivity |

```kotlin
try {
    client.produce("my-topic", value = "hello")
} catch (e: CircuitBreakerOpenException) {
    println("Circuit open for ${e.remainingMs}ms — ${e.hint}")
} catch (e: StreamlineException) {
    if (e.isRetryable) println("Transient: ${e.message} (${e.code})")
    else println("Fatal: ${e.message} — ${e.hint}")
}
```

### Circuit Breaker

The SDK wraps produce and subscribe operations in a `CircuitBreaker`. After consecutive failures exceed the threshold, the breaker opens and rejects calls immediately to prevent cascading failures:

```kotlin
val client = StreamlineClient(
    configuration = StreamlineConfiguration(
        url = "ws://localhost:9092",
        circuitBreakerConfig = CircuitBreakerConfig(
            failureThreshold = 5,   // Open after 5 consecutive failures
            resetTimeoutMs = 30_000, // Probe after 30 seconds
        ),
        retryPolicyConfig = RetryPolicyConfig(
            maxRetries = 3,
            baseDelayMs = 200,
            maxDelayMs = 30_000,
            jitter = true,
        ),
    ),
)
// Access breaker state programmatically
println("Circuit state: ${client.circuitBreaker.state}")
```

### Retry Policy

The `RetryPolicy` retries transient failures (where `isRetryable == true`) with exponential backoff and jitter. It wraps the circuit breaker — a `CircuitBreakerOpenException` is also retryable, so the retry policy will wait and probe again:

```kotlin
// Standalone usage outside the client
val policy = RetryPolicy(RetryPolicyConfig(maxRetries = 5, baseDelayMs = 100))
val result = policy.execute { riskyOperation() }
```

## Contributing

Contributions are welcome! This is a community-maintained SDK. Please see the [organization contributing guide](https://github.com/streamlinelabs/.github/blob/main/CONTRIBUTING.md) for guidelines.

## License

Apache-2.0
