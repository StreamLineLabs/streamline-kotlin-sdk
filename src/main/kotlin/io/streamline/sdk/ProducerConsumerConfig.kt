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
 * @property idempotent Retained for source compatibility; currently rejected, see [validate].
 * @property acks Acknowledgement level required from the server. Only [Acks.NONE] is currently
 * honored end-to-end, see [validate].
 */
data class ProducerConfig(
    val batchSize: Int = 16384,
    val lingerMs: Long = 0,
    val compression: CompressionType = CompressionType.NONE,
    val retries: Int = 3,
    val retryBackoffMs: Long = 100,
    val idempotent: Boolean = false,
    val acks: Acks = Acks.NONE,
) {
    /**
     * Validates that the acknowledgment contract this configuration claims can
     * actually be honored by the transport.
     *
     * The WebSocket produce command has no correlated per-message acknowledgment
     * response from the broker: a produce is considered "sent" as soon as the
     * local frame write succeeds, regardless of [acks]. Accepting [Acks.ONE] or
     * [Acks.ALL] would silently claim a broker-acknowledged delivery guarantee
     * the transport cannot verify, and accepting [idempotent] = true would
     * silently claim server-side deduplication that cannot be confirmed either.
     * Both are rejected here (fail closed) rather than accepted and violated.
     * [Acks.ONE]/[Acks.ALL] and `idempotent` are retained on this type only for
     * source compatibility; only [Acks.NONE] with a non-idempotent producer is
     * currently honored end-to-end.
     *
     * @throws ConfigurationException if [acks] is not [Acks.NONE] or [idempotent] is `true`.
     */
    fun validate() {
        if (acks != Acks.NONE) {
            throw ConfigurationException(
                "ProducerConfig.acks=$acks requires a correlated broker acknowledgment that the " +
                    "current WebSocket produce protocol does not provide; only Acks.NONE is honored " +
                    "end-to-end. Acks.ONE/ALL are retained for source compatibility but rejected so a " +
                    "delivery guarantee the transport cannot verify is never silently claimed.",
            )
        }
        if (idempotent) {
            throw ConfigurationException(
                "ProducerConfig.idempotent=true requires a correlated broker acknowledgment to confirm " +
                    "deduplication that the current WebSocket produce protocol does not provide; only a " +
                    "non-idempotent producer is honored end-to-end.",
            )
        }
    }
}

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
