# Streamline Kotlin SDK Examples

## Prerequisites

- Kotlin 1.9+ / JDK 17+
- A running Streamline server (`docker run -d -p 9092:9092 -p 9094:9094 ghcr.io/streamlinelabs/streamline:latest`)

## Examples

### Basic Usage

Demonstrates connecting, producing messages, consuming messages, and admin operations.

```bash
kotlinc -script BasicUsage.kt
```

### Schema Registry

Demonstrates registering schemas, producing with schema validation, and checking compatibility.

```bash
kotlinc -script SchemaRegistryUsage.kt
```

### SQL Queries

Demonstrates running SQL analytics queries on streaming data using the embedded DuckDB engine.

```bash
kotlinc -script QueryUsage.kt
```

## Configuration

By default, examples connect to `localhost:9092`. Set `STREAMLINE_BOOTSTRAP` to override:

```bash
STREAMLINE_BOOTSTRAP=my-server:9092 kotlinc -script BasicUsage.kt
```

## More Information

- [SDK Documentation](https://streamlinelabs.github.io/streamline-docs/docs/sdks/kotlin)
- [API Reference](https://github.com/streamlinelabs/streamline-kotlin-sdk#api-reference)
