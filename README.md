> 🟢 **Beta SDK** — This SDK is feature-complete with tests and CI. For production JVM use requiring Spring Boot integration, also see the [Java SDK](https://github.com/streamlinelabs/streamline-java-sdk). Contributions welcome!

# Streamline Kotlin SDK

[![CI](https://github.com/streamlinelabs/streamline-kotlin-sdk/actions/workflows/ci.yml/badge.svg)](https://github.com/streamlinelabs/streamline-kotlin-sdk/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Kotlin](https://img.shields.io/badge/Kotlin-2.0%2B-blue.svg)](https://kotlinlang.org/)
[![Docs](https://img.shields.io/badge/docs-streamlinelabs.dev-blue.svg)](https://streamlinelabs.dev/docs/sdks/kotlin)
[![Maven Central](https://img.shields.io/maven-central/v/io.streamline/streamline-sdk.svg)](https://search.maven.org/artifact/io.streamline/streamline-sdk)

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

## Transactions

```kotlin
val client = StreamlineClient(config)
client.connect()

client.beginTransaction()
try {
    client.produce("orders", key = "k1", value = "v1")
    client.produce("orders", key = "k2", value = "v2")
    client.commitTransaction()
} catch (e: Exception) {
    client.abortTransaction()
    throw e
}
```

> **Note:** Transactions use client-side buffering. Messages are collected and sent as a batch
> on commit, providing all-or-nothing delivery at the client level.

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

### Cluster & Monitoring

```kotlin
val admin = AdminClient("http://localhost:9094")

// Cluster overview
val cluster = admin.clusterInfo()
println("Cluster: ${cluster.clusterId}, Brokers: ${cluster.brokers.size}")
cluster.brokers.forEach { b -> println("  ${b.host}:${b.port} (rack=${b.rack})") }

// Consumer group lag monitoring
val lag = admin.consumerGroupLag("my-group")
println("Total lag: ${lag.totalLag}")
lag.partitions.forEach { p -> println("  ${p.topic}:${p.partition} lag=${p.lag}") }

// Topic-scoped lag
val topicLag = admin.consumerGroupTopicLag("my-group", "events")

// Message inspection
val messages = admin.inspectMessages("events", partition = 0, limit = 10)
messages.forEach { m -> println("offset=${m.offset} key=${m.key} value=${m.value}") }

// Latest messages
val latest = admin.latestMessages("events", count = 5)

// Server metrics
val metrics = admin.metricsHistory()
metrics.forEach { m -> println("${m.name}=${m.value} ${m.labels}") }

// Offset management
val dryRun = admin.resetOffsetsDryRun("my-group", "events", "earliest")
admin.resetOffsets("my-group", "events", "earliest")

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

All SDK exceptions extend `StreamlineException`. Catch specific types for targeted error handling:

| Exception | Description | Retryable? |
|-----------|-------------|------------|
| `NotConnectedException` | Client is not connected | Yes — reconnects automatically |
| `ConnectionFailedException` | Connection attempt failed | Yes — retry with backoff |
| `AuthenticationFailedException` | Server rejected credentials | No |
| `StreamlineTimeoutException` | Operation timed out | Yes |
| `TopicNotFoundException` | Requested topic does not exist | No — create the topic first |
| `OfflineQueueFullException` | Offline buffer capacity exceeded | No — reduce send rate |
| `AdminOperationException` | Admin API call failed | Depends on cause |
| `QueryException` | SQL query execution failed | Depends on cause |
| `SchemaRegistryException` | Schema registry operation failed | Depends on cause |

```kotlin
try {
    client.produce("my-topic", value = "hello")
} catch (e: TopicNotFoundException) {
    println("Topic not found: ${e.message}")
} catch (e: ConnectionFailedException) {
    println("Connection failed: ${e.message}")
    // Retryable — client will auto-reconnect
} catch (e: AuthenticationFailedException) {
    println("Auth failed (not retryable): ${e.message}")
} catch (e: OfflineQueueFullException) {
    println("Queue full — reduce send rate")
} catch (e: StreamlineException) {
    println("Streamline error: ${e.message}")
}
```

### Retry Strategy

The Kotlin SDK automatically retries failed sends with exponential backoff when `ProducerConfig.retries > 0` (default: 3). Configure retry behavior:

```kotlin
val client = StreamlineClient(
    configuration = StreamlineConfiguration(url = "ws://localhost:9092"),
)
client.producerConfig = ProducerConfig(
    retries = 5,             // Max retry attempts
    retryBackoffMs = 200L,   // Base backoff (doubles each attempt)
    compression = CompressionType.ZSTD,
    batchSize = 32768,       // 32KB batches
    lingerMs = 10L,          // 10ms batch window
)
```

## Circuit Breaker

Protect your application from cascading failures when the Streamline server is unresponsive:

```kotlin
import io.streamline.sdk.CircuitBreaker
import io.streamline.sdk.CircuitBreakerConfig
import io.streamline.sdk.CircuitState

val breaker = CircuitBreaker(CircuitBreakerConfig(
    failureThreshold = 5,        // Open after 5 consecutive failures
    successThreshold = 2,        // Close after 2 half-open successes
    openTimeoutMs = 30_000L,     // 30s before probing
    onStateChange = { from, to -> println("Circuit: $from → $to") },
))

// Wrap a suspending operation
val result = breaker.execute {
    client.produce("events", value = """{"action":"click"}""")
}

// Or check state manually
if (breaker.state == CircuitState.OPEN) {
    println("Circuit is open — requests will be rejected")
}
```

When the circuit is open, `execute` throws a retryable `StreamlineException`. See the [Circuit Breaker guide](https://streamlinelabs.dev/docs/features/circuit-breaker) for details.

## Examples

The [`examples/`](examples/) directory contains runnable examples:

| Example | Description |
|---------|-------------|
| [BasicUsage.kt](examples/BasicUsage.kt) | Produce, consume, and admin operations |
| [QueryUsage.kt](examples/QueryUsage.kt) | SQL analytics with the embedded query engine |
| [SchemaRegistryUsage.kt](examples/SchemaRegistryUsage.kt) | Schema registration and validation |
| [CircuitBreakerUsage.kt](examples/CircuitBreakerUsage.kt) | Resilient production with circuit breaker |
| [SecurityUsage.kt](examples/SecurityUsage.kt) | TLS and SASL authentication |

## Moonshot Features

> ⚠️ **Experimental** — These features require Streamline server 0.3.0+ with moonshot feature flags enabled.

### Semantic Search

Query topics by meaning instead of offset. Requires a topic created with `semantic.embed=true`.

```kotlin
val results = client.search("logs.app", "payment failure", k = 10)
for (hit in results) {
    println("[p${hit.partition}] offset=${hit.offset} score=${"%.2f".format(hit.score)}")
}
```

### Attestation Verification

Verify cryptographic provenance attestations attached to records by data contracts.

```kotlin
import io.streamline.sdk.StreamlineVerifier

val verifier = StreamlineVerifier(publicKeyBytes)
val result = verifier.verify(record)
println("Verified: ${result.verified}, Producer: ${result.producerId}")
```

### Agent Memory (MCP)

Use Streamline as persistent memory for AI agents via the MCP protocol.

```kotlin
import io.streamline.sdk.MemoryClient

val memory = MemoryClient("http://localhost:9094/mcp/v1")
memory.remember("user prefers dark mode", tags = listOf("preferences"))
val results = memory.recall("user preferences", k = 5)
```

### Branched Streams

Create topic branches for replay, A/B testing, or counterfactual analysis.

```kotlin
val branch = admin.createBranch("events", "experiment-v2")
client.messages(branch.topic).collect { msg ->
    println("Branched: ${msg.value}")
}
```

## Contributing

Contributions are welcome! This is a community-maintained SDK. Please see the [organization contributing guide](https://github.com/streamlinelabs/.github/blob/main/CONTRIBUTING.md) for guidelines.

## License

Apache-2.0

