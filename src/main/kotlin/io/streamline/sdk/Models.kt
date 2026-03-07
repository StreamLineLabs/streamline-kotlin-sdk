package io.streamline.sdk

import kotlinx.serialization.Serializable
import java.time.Instant

// -- Connection State --

/** Represents the current state of the client's connection to the server. */
enum class ConnectionState {
    DISCONNECTED,
    CONNECTING,
    CONNECTED,
    RECONNECTING,
}

// -- Configuration --

/**
 * Configuration for connecting to a Streamline server.
 *
 * @property url WebSocket URL of the Streamline server (e.g. "ws://localhost:9092").
 * @property autoReconnect Whether to automatically reconnect on disconnection.
 * @property maxRetries Maximum number of reconnection attempts.
 * @property timeoutMs Connection timeout in milliseconds.
 * @property authToken Optional bearer token for authentication.
 * @property initialBackoffMs Initial reconnection backoff in milliseconds.
 * @property maxBackoffMs Maximum backoff cap in milliseconds.
 * @property tls Optional TLS configuration for encrypted connections.
 * @property sasl Optional SASL configuration for authentication.
 * @property circuitBreakerConfig Configuration for the circuit breaker protecting send operations.
 * @property retryPolicyConfig Configuration for the retry policy on transient failures.
 */
data class StreamlineConfiguration(
    val url: String,
    val autoReconnect: Boolean = true,
    val maxRetries: Int = 10,
    val timeoutMs: Long = 30_000,
    val authToken: String? = null,
    val initialBackoffMs: Long = 500,
    val maxBackoffMs: Long = 30_000,
    val tls: TlsConfig? = null,
    val sasl: SaslConfig? = null,
    val circuitBreakerConfig: CircuitBreakerConfig = CircuitBreakerConfig(),
    val retryPolicyConfig: RetryPolicyConfig = RetryPolicyConfig(),
)

// -- Messages --

/** A message produced to or consumed from a Streamline topic. */
@Serializable
data class StreamlineMessage(
    val topic: String,
    val key: String? = null,
    val value: String,
    val offset: Long? = null,
    val timestamp: Long? = null,
)

// -- Topic Info --

/** Metadata about a Streamline topic. */
@Serializable
data class TopicInfo(
    val name: String,
    val partitions: Int,
    val replicationFactor: Int,
    val messageCount: Long,
)

// -- Consumer Group --

/** Represents a consumer group on the server. */
@Serializable
data class ConsumerGroup(
    val id: String,
    val members: List<String>,
    val state: String,
)

// -- Topic Configuration --

/** Configuration for creating a new topic. */
data class CreateTopicRequest(
    val name: String,
    val partitions: Int = 1,
    val replicationFactor: Int = 1,
    val config: Map<String, String> = emptyMap(),
)

/** Detailed topic description including partition assignments. */
@Serializable
data class TopicDescription(
    val name: String,
    val partitions: Int,
    val replicationFactor: Int,
    val messageCount: Long = 0,
    val config: Map<String, String> = emptyMap(),
)

// -- Consumer Group Details --

/** A member of a consumer group. */
@Serializable
data class ConsumerGroupMember(
    val id: String,
    val clientId: String = "",
    val host: String = "",
    val assignments: List<String> = emptyList(),
)

/** Detailed description of a consumer group. */
@Serializable
data class ConsumerGroupDescription(
    val id: String,
    val state: String,
    val members: List<ConsumerGroupMember> = emptyList(),
    val protocol: String = "",
)

// -- Query --

/** Result from a SQL query against the streaming data. */
@Serializable
data class QueryResult(
    val columns: List<String> = emptyList(),
    val rows: List<List<String>> = emptyList(),
    val rowCount: Int = 0,
)

// -- Server Info --

/** Information about the Streamline server. */
@Serializable
data class ServerInfo(
    val version: String = "",
    val uptime: Long = 0,
    val topicCount: Int = 0,
    val messageCount: Long = 0,
)

// -- Error Codes --

/** Categorizes SDK errors for programmatic handling. */
enum class ErrorCode {
    CONNECTION,
    AUTHENTICATION,
    AUTHORIZATION,
    TOPIC_NOT_FOUND,
    TIMEOUT,
    PRODUCER,
    CONSUMER,
    SERIALIZATION,
    CONFIG,
    CIRCUIT_BREAKER_OPEN,
    OFFLINE_QUEUE_FULL,
    ADMIN,
    QUERY,
    SCHEMA_REGISTRY,
    INTERNAL,
}

// -- Errors --

/**
 * Base exception for all Streamline SDK errors.
 *
 * @property code Machine-readable error category for programmatic handling.
 * @property hint A human-friendly suggestion for resolving the error.
 * @property isRetryable Whether the operation may succeed if retried.
 */
open class StreamlineException(
    message: String,
    cause: Throwable? = null,
    val code: ErrorCode = ErrorCode.INTERNAL,
    val hint: String? = null,
    val isRetryable: Boolean = false,
) : Exception(message, cause)

class NotConnectedException : StreamlineException(
    "Client is not connected",
    code = ErrorCode.CONNECTION,
    hint = "Call connect() before performing operations.",
    isRetryable = true,
)

class ConnectionFailedException(message: String, cause: Throwable? = null) : StreamlineException(
    message, cause,
    code = ErrorCode.CONNECTION,
    hint = "Check that the Streamline server is running and the URL is correct.",
    isRetryable = true,
)

class AuthenticationFailedException(message: String) : StreamlineException(
    message,
    code = ErrorCode.AUTHENTICATION,
    hint = "Verify your auth token or SASL credentials.",
    isRetryable = false,
)

class AuthorizationFailedException(message: String) : StreamlineException(
    message,
    code = ErrorCode.AUTHORIZATION,
    hint = "Check that the authenticated principal has the required ACL permissions.",
    isRetryable = false,
)

class StreamlineTimeoutException(message: String = "Operation timed out") : StreamlineException(
    message,
    code = ErrorCode.TIMEOUT,
    hint = "Increase the timeout or check server health.",
    isRetryable = true,
)

class TopicNotFoundException(topic: String) : StreamlineException(
    "Topic not found: $topic",
    code = ErrorCode.TOPIC_NOT_FOUND,
    hint = "Create the topic first or check for typos in the topic name.",
    isRetryable = false,
)

class OfflineQueueFullException : StreamlineException(
    "Offline queue is full",
    code = ErrorCode.OFFLINE_QUEUE_FULL,
    hint = "Reconnect to the server or increase the offline queue capacity.",
    isRetryable = false,
)

class CircuitBreakerOpenException(val remainingMs: Long = 0) : StreamlineException(
    "Circuit breaker is open — requests are blocked for ${remainingMs}ms",
    code = ErrorCode.CIRCUIT_BREAKER_OPEN,
    hint = "The remote endpoint is unhealthy. Retry after the reset timeout.",
    isRetryable = true,
)

class ProducerException(message: String, cause: Throwable? = null) : StreamlineException(
    message, cause,
    code = ErrorCode.PRODUCER,
    hint = "Check message size limits and server connectivity.",
    isRetryable = true,
)

class ConsumerException(message: String, cause: Throwable? = null) : StreamlineException(
    message, cause,
    code = ErrorCode.CONSUMER,
    hint = "Check consumer group configuration and server connectivity.",
    isRetryable = true,
)

class SerializationException(message: String, cause: Throwable? = null) : StreamlineException(
    message, cause,
    code = ErrorCode.SERIALIZATION,
    hint = "Verify message format matches the expected schema.",
    isRetryable = false,
)

class AdminOperationException(message: String, cause: Throwable? = null) : StreamlineException(
    message, cause,
    code = ErrorCode.ADMIN,
    hint = "Check server connectivity and required permissions.",
    isRetryable = true,
)

class QueryException(message: String, cause: Throwable? = null) : StreamlineException(
    message, cause,
    code = ErrorCode.QUERY,
    hint = "Verify SQL syntax and that the analytics feature is enabled on the server.",
    isRetryable = false,
)

class SchemaRegistryException(message: String, cause: Throwable? = null) : StreamlineException(
    message, cause,
    code = ErrorCode.SCHEMA_REGISTRY,
    hint = "Check schema registry connectivity and schema compatibility.",
    isRetryable = true,
)

// -- Schema Registry --

/** Supported schema serialization formats. */
enum class SchemaFormat { AVRO, PROTOBUF, JSON }

/** Metadata about a schema stored in the registry. */
@Serializable
data class SchemaInfo(
    val subject: String,
    val id: Int,
    val version: Int,
    val schemaType: String = "AVRO",
    val schema: String,
)
