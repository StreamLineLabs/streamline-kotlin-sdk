package io.streamline.sdk

/** Compression algorithm for produced messages. */
enum class CompressionType { NONE, GZIP, SNAPPY, LZ4, ZSTD }

/** Acknowledgement level required from the server before a produce is considered successful. */
enum class Acks { NONE, ONE, ALL }

/**
 * Configuration for message production.
 *
 * @property batchSize Maximum size (in bytes) of a message batch before sending.
 * @property lingerMs Maximum time to wait for additional messages before sending a batch.
 * @property compression Compression algorithm applied to message batches.
 * @property retries Number of times to retry a failed produce request.
 * @property retryBackoffMs Delay between produce retries in milliseconds.
 * @property idempotent Enable idempotent production to prevent duplicates.
 * @property acks Acknowledgement level required from the server.
 */
data class ProducerConfig(
    val batchSize: Int = 16384,
    val lingerMs: Long = 0,
    val compression: CompressionType = CompressionType.NONE,
    val retries: Int = 3,
    val retryBackoffMs: Long = 100,
    val idempotent: Boolean = false,
    val acks: Acks = Acks.ONE,
)

/** Strategy for resetting the consumer offset when no committed offset is found. */
enum class OffsetReset { EARLIEST, LATEST, NONE }

/**
 * Configuration for message consumption.
 *
 * @property groupId Consumer group identifier. When null, no group coordination is performed.
 * @property autoCommit Whether offsets are committed automatically after polling.
 * @property autoCommitIntervalMs Interval between automatic offset commits in milliseconds.
 * @property sessionTimeoutMs Maximum time before a consumer is considered dead by the group coordinator.
 * @property heartbeatIntervalMs Interval between heartbeats sent to the group coordinator.
 * @property maxPollRecords Maximum number of records returned in a single poll.
 * @property autoOffsetReset Strategy when no committed offset exists for a partition.
 */
data class ConsumerConfig(
    val groupId: String? = null,
    val autoCommit: Boolean = true,
    val autoCommitIntervalMs: Long = 5000,
    val sessionTimeoutMs: Long = 30000,
    val heartbeatIntervalMs: Long = 3000,
    val maxPollRecords: Int = 500,
    val autoOffsetReset: OffsetReset = OffsetReset.LATEST,
)
