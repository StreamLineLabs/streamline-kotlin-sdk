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
 */
data class StreamlineConfiguration(
    val url: String,
    val autoReconnect: Boolean = true,
    val maxRetries: Int = 10,
    val timeoutMs: Long = 30_000,
    val authToken: String? = null,
    val initialBackoffMs: Long = 500,
    val maxBackoffMs: Long = 30_000,
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

// -- Errors --

/** Base exception for all Streamline SDK errors. */
open class StreamlineException(message: String, cause: Throwable? = null) : Exception(message, cause)

class NotConnectedException : StreamlineException("Client is not connected")
class ConnectionFailedException(message: String, cause: Throwable? = null) : StreamlineException(message, cause)
class AuthenticationFailedException(message: String) : StreamlineException(message)
class StreamlineTimeoutException : StreamlineException("Operation timed out")
class TopicNotFoundException(topic: String) : StreamlineException("Topic not found: $topic")
class OfflineQueueFullException : StreamlineException("Offline queue is full")
