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

// -- Errors --

/** Base exception for all Streamline SDK errors. */
open class StreamlineException(message: String, cause: Throwable? = null) : Exception(message, cause)

class NotConnectedException : StreamlineException("Client is not connected")
class ConnectionFailedException(message: String, cause: Throwable? = null) : StreamlineException(message, cause)
class AuthenticationFailedException(message: String) : StreamlineException(message)
class StreamlineTimeoutException : StreamlineException("Operation timed out")
class TopicNotFoundException(topic: String) : StreamlineException("Topic not found: $topic")
class OfflineQueueFullException : StreamlineException("Offline queue is full")
class AdminOperationException(message: String, cause: Throwable? = null) : StreamlineException(message, cause)
class QueryException(message: String, cause: Throwable? = null) : StreamlineException(message, cause)
class SchemaRegistryException(message: String, cause: Throwable? = null) : StreamlineException(message, cause)

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
