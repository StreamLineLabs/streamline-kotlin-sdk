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
    val partition: Int? = null,
    val offset: Long? = null,
    val timestamp: Long? = null,
    val headers: Map<String, String> = emptyMap(),
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

// -- Search --

/** A single search result from a topic. */
@Serializable
data class SearchResult(
    val partition: Int,
    val offset: Long,
    val score: Double,
    val value: String? = null,
)

/** Internal response from the search API. */
@Serializable
internal data class SearchApiResponse(
    val hits: List<SearchResult> = emptyList(),
    @kotlinx.serialization.SerialName("took_ms")
    val tookMs: Long = 0,
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

/** Programmatic classification of Streamline SDK errors. */
enum class ErrorCode {
    CONNECTION,
    TIMEOUT,
    AUTHENTICATION,
    AUTHORIZATION,
    TOPIC_NOT_FOUND,
    PARTITION_NOT_FOUND,
    PROTOCOL,
    SERIALIZATION,
    SCHEMA,
    CONFIGURATION,
    INTERNAL,
    CIRCUIT_OPEN,
    CONTRACT_VIOLATION,
    ATTESTATION_FAILED,
    MEMORY_ACCESS_DENIED,
    BRANCH_QUOTA_EXCEEDED,
    SEMANTIC_SEARCH_UNAVAILABLE;

    /** Whether errors of this code are generally safe to retry. */
    val defaultRetryable: Boolean
        get() = when (this) {
            CONNECTION, TIMEOUT, INTERNAL, SEMANTIC_SEARCH_UNAVAILABLE -> true
            AUTHENTICATION, AUTHORIZATION, TOPIC_NOT_FOUND, PARTITION_NOT_FOUND,
            PROTOCOL, SERIALIZATION, SCHEMA, CONFIGURATION, CIRCUIT_OPEN,
            CONTRACT_VIOLATION, ATTESTATION_FAILED, MEMORY_ACCESS_DENIED, BRANCH_QUOTA_EXCEEDED -> false
        }
}

// -- Errors --

/** Base exception for all Streamline SDK errors. */
open class StreamlineException(
    message: String,
    cause: Throwable? = null,
    val errorCode: ErrorCode = ErrorCode.INTERNAL,
    val retryable: Boolean = errorCode.defaultRetryable,
    val hint: String? = null,
) : Exception(message, cause) {
    /** Returns whether this error is safe to retry. */
    fun isRetryable(): Boolean = retryable
}

class NotConnectedException : StreamlineException(
    message = "Client is not connected",
    errorCode = ErrorCode.CONNECTION,
    hint = "Call connect() before performing operations",
)

class ConnectionFailedException(message: String, cause: Throwable? = null) : StreamlineException(
    message = message,
    cause = cause,
    errorCode = ErrorCode.CONNECTION,
    hint = "Check that the server is running and the URL is correct",
)

class AuthenticationFailedException(message: String) : StreamlineException(
    message = message,
    errorCode = ErrorCode.AUTHENTICATION,
    hint = "Verify your auth token or SASL credentials",
)

class AuthorizationFailedException(message: String) : StreamlineException(
    message = message,
    errorCode = ErrorCode.AUTHORIZATION,
    hint = "Ensure the authenticated user has the required permissions",
)

class StreamlineTimeoutException(message: String = "Operation timed out") : StreamlineException(
    message = message,
    errorCode = ErrorCode.TIMEOUT,
    hint = "Increase timeoutMs or check server responsiveness",
)

class TopicNotFoundException(topic: String) : StreamlineException(
    message = "Topic not found: $topic",
    errorCode = ErrorCode.TOPIC_NOT_FOUND,
    hint = "Create the topic first or check the topic name",
)

class PartitionNotFoundException(topic: String, partition: Int) : StreamlineException(
    message = "Partition $partition not found for topic: $topic",
    errorCode = ErrorCode.PARTITION_NOT_FOUND,
    hint = "Verify the partition number is within the topic's partition count",
)

class ProtocolException(message: String, cause: Throwable? = null) : StreamlineException(
    message = message,
    cause = cause,
    errorCode = ErrorCode.PROTOCOL,
    hint = "This may indicate a version mismatch between SDK and server",
)

class SerializationException(message: String, cause: Throwable? = null) : StreamlineException(
    message = message,
    cause = cause,
    errorCode = ErrorCode.SERIALIZATION,
    hint = "Check that the message format matches the expected schema",
)

class OfflineQueueFullException : StreamlineException(
    message = "Offline queue is full",
    errorCode = ErrorCode.INTERNAL,
    retryable = false,
    hint = "Wait for the connection to be restored or increase queue capacity",
)

class AdminOperationException(message: String, cause: Throwable? = null) : StreamlineException(
    message = message,
    cause = cause,
    errorCode = ErrorCode.INTERNAL,
)

class QueryException(message: String, cause: Throwable? = null) : StreamlineException(
    message = message,
    cause = cause,
    errorCode = ErrorCode.INTERNAL,
)

class SchemaRegistryException(message: String, cause: Throwable? = null) : StreamlineException(
    message = message,
    cause = cause,
    errorCode = ErrorCode.SCHEMA,
)

class CircuitOpenException(message: String = "Circuit breaker is open") : StreamlineException(
    message = message,
    errorCode = ErrorCode.CIRCUIT_OPEN,
    retryable = false,
    hint = "The circuit breaker has tripped; wait for the open timeout before retrying",
)

class ConfigurationException(message: String) : StreamlineException(
    message = message,
    errorCode = ErrorCode.CONFIGURATION,
    hint = "Review the SDK configuration values",
)

class ContractViolationException(topic: String, details: String) : StreamlineException(
    message = "Contract violation on topic '$topic': $details",
    errorCode = ErrorCode.CONTRACT_VIOLATION,
    hint = "Validate the record against the topic's registered schema",
)

class AttestationVerificationException(message: String, cause: Throwable? = null) : StreamlineException(
    message = message,
    cause = cause,
    errorCode = ErrorCode.ATTESTATION_FAILED,
    hint = "Check the signing key and attestation configuration",
)

class MemoryAccessDeniedException(agent: String) : StreamlineException(
    message = "Memory access denied for agent: $agent",
    errorCode = ErrorCode.MEMORY_ACCESS_DENIED,
    hint = "Verify agent permissions for memory operations",
)

class BranchQuotaExceededException(branch: String, details: String) : StreamlineException(
    message = "Branch quota exceeded for '$branch': $details",
    errorCode = ErrorCode.BRANCH_QUOTA_EXCEEDED,
    hint = "Increase branch quotas or clean up unused branches",
)

class SemanticSearchUnavailableException(message: String, cause: Throwable? = null) : StreamlineException(
    message = message,
    cause = cause,
    errorCode = ErrorCode.SEMANTIC_SEARCH_UNAVAILABLE,
    hint = "Check embedding provider connectivity and configuration",
)

// -- Cluster Info --

/** Information about the Streamline cluster. */
@Serializable
data class ClusterInfo(
    val clusterId: String = "",
    val brokerId: Int = 0,
    val brokers: List<BrokerInfo> = emptyList(),
    val controller: Int = -1,
)

/** Information about a single broker in the cluster. */
@Serializable
data class BrokerInfo(
    val id: Int = 0,
    val host: String = "",
    val port: Int = 9092,
    val rack: String? = null,
)

// -- Consumer Lag --

/** Consumer group lag information for a topic partition. */
@Serializable
data class ConsumerLag(
    val topic: String,
    val partition: Int = 0,
    val currentOffset: Long = 0,
    val endOffset: Long = 0,
    val lag: Long = 0,
)

/** Aggregated consumer group lag across all subscribed partitions. */
@Serializable
data class ConsumerGroupLag(
    val groupId: String,
    val partitions: List<ConsumerLag> = emptyList(),
    val totalLag: Long = 0,
)

// -- Message Inspection --

/** A message returned by the message inspection API. */
@Serializable
data class InspectedMessage(
    val offset: Long,
    val key: String? = null,
    val value: String = "",
    val timestamp: Long = 0,
    val partition: Int = 0,
    val headers: Map<String, String> = emptyMap(),
)

// -- Metrics --

/** A single metric data point from the server. */
@Serializable
data class MetricPoint(
    val name: String = "",
    val value: Double = 0.0,
    val labels: Map<String, String> = emptyMap(),
    val timestamp: Long = 0,
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

/** Schema compatibility enforcement levels. */
enum class CompatibilityLevel {
    BACKWARD,
    FORWARD,
    FULL,
    NONE,
    BACKWARD_TRANSITIVE,
    FORWARD_TRANSITIVE,
    FULL_TRANSITIVE,
}

// -- ACL --

/** ACL resource types that can be secured. */
enum class AclResourceType { TOPIC, GROUP, CLUSTER, TRANSACTIONAL_ID }

/** ACL permission types. */
enum class AclPermission { ALLOW, DENY }

/** ACL operation types. */
enum class AclOperation { READ, WRITE, CREATE, DELETE, ALTER, DESCRIBE, ALL }

/** An Access Control List entry. */
@Serializable
data class AclEntry(
    val principal: String,
    val resourceType: String,
    val resourceName: String,
    val operation: String,
    val permission: String,
    val host: String = "*",
)

// -- Partition Reassignment --

/** Status of an in-progress partition reassignment. */
@Serializable
data class ReassignmentStatus(
    val topic: String,
    val partition: Int,
    val replicas: List<Int> = emptyList(),
    val addingReplicas: List<Int> = emptyList(),
    val removingReplicas: List<Int> = emptyList(),
)
