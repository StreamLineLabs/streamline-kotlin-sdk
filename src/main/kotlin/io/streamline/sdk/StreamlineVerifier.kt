package io.streamline.sdk

import java.nio.charset.StandardCharsets
import java.security.InvalidKeyException
import java.security.MessageDigest
import java.security.NoSuchAlgorithmException
import java.security.PublicKey
import java.security.Signature
import java.security.SignatureException
import java.util.Base64
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json

/**
 * Result of an attestation verification.
 *
 * @property verified Whether the Ed25519 signature was valid.
 * @property producerId The trusted key ID authenticated by this result.
 *   Failed verification results do not expose an untrusted envelope identity.
 * @property schemaId Schema id (null when zero / absent).
 * @property contractId Optional contract id.
 * @property timestampMs Attestation timestamp in epoch milliseconds.
 */
data class VerificationResult(
    val verified: Boolean,
    val producerId: String = "",
    val schemaId: Int? = null,
    val contractId: String? = null,
    val timestampMs: Long = 0,
) {
    companion object {
        internal fun failed() = VerificationResult(verified = false)
    }
}

/**
 * Parsed attestation envelope from the `streamline-attest` header.
 */
@Serializable
internal data class AttestationEnvelope(
    @SerialName("payload_sha256") val payloadSha256: String,
    val topic: String,
    val partition: Int,
    val offset: Long,
    @SerialName("schema_id") val schemaId: Int,
    @SerialName("timestamp_ms") val timestampMs: Long,
    @SerialName("key_id") val keyId: String,
    val signature: String,
    @SerialName("contract_id") val contractId: String? = null,
)

/**
 * Resolves the trusted Ed25519 public key for a given attestation `key_id`.
 *
 * Implementations must return `null` for any `key_id` that is not a known,
 * trusted identity. [StreamlineVerifier] treats a `null` result as a failed
 * verification: it never falls back to verifying against some other key, and
 * it never treats the envelope's self-declared `key_id` as authenticated
 * unless this resolver recognizes it. This is what binds the envelope's
 * claimed producer identity to a specific, independently trusted key rather
 * than accepting whatever identity a signer chooses to assert.
 */
fun interface TrustedKeyResolver {
    /** Returns the trusted public key for [keyId], or `null` if untrusted/unknown. */
    fun resolve(keyId: String): PublicKey?
}

/**
 * Verifies `streamline-attest` headers on consumed messages using a local
 * Ed25519 public key. No network calls are made.
 *
 * The attestation header contains a Base64-encoded JSON envelope with an
 * Ed25519 signature over the canonical bytes:
 * `topic|partition|offset|payload_sha256|schema_id|timestamp_ms|key_id`.
 *
 * Multi-producer / key-rotation deployments should bind the envelope's
 * `key_id` to a specific trusted key, either via a single expected identity:
 *
 * ```kotlin
 * val verifier = StreamlineVerifier(pubKey, expectedKeyId = "producer-key-1")
 * ```
 *
 * or via a resolver backed by a trust store keyed by `key_id`:
 *
 * ```kotlin
 * val verifier = StreamlineVerifier(TrustedKeyResolver { keyId -> trustStore[keyId] })
 * ```
 *
 * In both forms, [VerificationResult.verified] is only `true` when the
 * signature verifies against the key registered for the *claimed* `key_id` —
 * an attacker holding a different trusted key cannot impersonate another
 * producer by simply relabeling `key_id`, because verification uses the key
 * looked up for that label, not just any known key.
 *
 * The legacy single-key constructor remains source-compatible but fails
 * verification closed because no trusted `key_id` was supplied.
 *
 * @param publicKey An Ed25519 public key.
 */
class StreamlineVerifier(
    /**
     * Resolves the trusted key for an envelope's `key_id`. Use the
     * `(publicKey, expectedKeyId)` constructor for one trusted identity; this
     * primary constructor supports key rotation and multiple producers.
     */
    private val keyResolver: TrustedKeyResolver,
) {
    /**
     * Binds attestation verification to a single expected producer identity.
     * Envelopes whose `key_id` does not equal [expectedKeyId] are rejected
     * before any cryptographic check is attempted (fail-closed).
     *
     * @param publicKey The Ed25519 public key trusted for [expectedKeyId].
     * @param expectedKeyId The only `key_id` this verifier will accept.
     */
    constructor(publicKey: PublicKey, expectedKeyId: String) : this(
        TrustedKeyResolver { keyId -> if (keyId == expectedKeyId) publicKey else null },
    )

    /**
     * Legacy source-compatible constructor without a trusted `key_id`.
     *
     * Verification always fails closed in this mode because accepting an
     * arbitrary envelope `key_id` would authenticate a self-declared
     * identity. Use
     * [StreamlineVerifier(PublicKey, String)][StreamlineVerifier] or
     * [StreamlineVerifier(TrustedKeyResolver)][StreamlineVerifier] to bind the
     * signing key to an independently trusted identity.
     */
    @Deprecated(message = LEGACY_CONSTRUCTOR_MESSAGE)
    constructor(
        @Suppress("UNUSED_PARAMETER") publicKey: PublicKey,
    ) : this(TrustedKeyResolver { null })

    private val json = Json { ignoreUnknownKeys = true }

    /**
     * Verify the attestation on a [StreamlineMessage].
     *
     * @param message The consumed message to verify.
     * @return A [VerificationResult] indicating success or failure.
     */
    fun verify(message: StreamlineMessage): VerificationResult {
        val headerValue =
            message.headers[ATTEST_HEADER]
                ?: return VerificationResult.failed()

        val envelope: AttestationEnvelope =
            try {
                val decoded = Base64.getDecoder().decode(headerValue)
                json.decodeFromString<AttestationEnvelope>(String(decoded, StandardCharsets.UTF_8))
            } catch (_: Exception) {
                return VerificationResult.failed()
            }

        // Fail closed: never verify against an arbitrary/self-declared key.
        // The public key used below is always the one bound to this exact
        // key_id by the resolver, never a fallback.
        val trustedKey =
            keyResolver.resolve(envelope.keyId)
                ?: return VerificationResult.failed()

        if (!matchesMessage(envelope, message)) {
            return VerificationResult.failed()
        }

        val canonical =
            listOf(
                envelope.topic,
                envelope.partition,
                envelope.offset,
                envelope.payloadSha256,
                envelope.schemaId,
                envelope.timestampMs,
                envelope.keyId,
            ).joinToString("|")

        val signatureBytes =
            try {
                Base64.getDecoder().decode(envelope.signature)
            } catch (_: IllegalArgumentException) {
                return VerificationResult.failed()
            }

        val verified =
            try {
                val sig = Signature.getInstance("Ed25519")
                sig.initVerify(trustedKey)
                sig.update(canonical.toByteArray(StandardCharsets.UTF_8))
                sig.verify(signatureBytes)
            } catch (_: NoSuchAlgorithmException) {
                false
            } catch (_: InvalidKeyException) {
                false
            } catch (_: SignatureException) {
                false
            }

        if (!verified) {
            return VerificationResult.failed()
        }
        return VerificationResult(
            verified = true,
            producerId = envelope.keyId,
            schemaId = if (envelope.schemaId != 0) envelope.schemaId else null,
            contractId = envelope.contractId,
            timestampMs = envelope.timestampMs,
        )
    }

    private fun matchesMessage(
        envelope: AttestationEnvelope,
        message: StreamlineMessage,
    ): Boolean {
        if (
            message.topic != envelope.topic ||
            message.partition != envelope.partition ||
            message.offset != envelope.offset
        ) {
            return false
        }

        val expectedHash =
            envelope.payloadSha256.decodeHexSha256()
                ?: return false
        val actualHash =
            MessageDigest.getInstance("SHA-256")
                .digest(message.value.toByteArray(StandardCharsets.UTF_8))
        return MessageDigest.isEqual(expectedHash, actualHash)
    }

    private fun String.decodeHexSha256(): ByteArray? {
        if (length != SHA_256_HEX_LENGTH || any { it.digitToIntOrNull(16) == null }) {
            return null
        }
        return ByteArray(SHA_256_BYTE_LENGTH) { index ->
            substring(index * 2, index * 2 + 2).toInt(16).toByte()
        }
    }

    companion object {
        /** Kafka header name carrying the attestation envelope. */
        const val ATTEST_HEADER = "streamline-attest"

        private const val SHA_256_BYTE_LENGTH = 32
        private const val SHA_256_HEX_LENGTH = SHA_256_BYTE_LENGTH * 2
        private const val LEGACY_CONSTRUCTOR_MESSAGE =
            "Verification fails closed without a trusted key_id. " +
                "Use StreamlineVerifier(publicKey, expectedKeyId) or StreamlineVerifier(resolver)."
    }
}
