package io.streamline.sdk

import java.nio.charset.StandardCharsets
import java.security.InvalidKeyException
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
 * @property producerId The key_id from the attestation envelope.
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
 * Verifies `streamline-attest` headers on consumed messages using a local
 * Ed25519 public key. No network calls are made.
 *
 * The attestation header contains a Base64-encoded JSON envelope with an
 * Ed25519 signature over the canonical bytes:
 * `topic|partition|offset|payload_sha256|schema_id|timestamp_ms|key_id`.
 *
 * Example:
 * ```kotlin
 * val keyFactory = KeyFactory.getInstance("Ed25519")
 * val pubKey = keyFactory.generatePublic(X509EncodedKeySpec(derBytes))
 * val verifier = StreamlineVerifier(pubKey)
 *
 * val result = verifier.verify(message)
 * if (result.verified) {
 *     println("Verified from ${result.producerId}")
 * }
 * ```
 *
 * @param publicKey An Ed25519 public key.
 */
class StreamlineVerifier(private val publicKey: PublicKey) {

    private val json = Json { ignoreUnknownKeys = true }

    /**
     * Verify the attestation on a [StreamlineMessage].
     *
     * @param message The consumed message to verify.
     * @return A [VerificationResult] indicating success or failure.
     */
    fun verify(message: StreamlineMessage): VerificationResult {
        val headerValue = message.headers[ATTEST_HEADER]
            ?: return VerificationResult.failed()

        val envelope: AttestationEnvelope = try {
            val decoded = Base64.getDecoder().decode(headerValue)
            json.decodeFromString<AttestationEnvelope>(String(decoded, StandardCharsets.UTF_8))
        } catch (_: Exception) {
            return VerificationResult.failed()
        }

        val canonical = listOf(
            envelope.topic,
            envelope.partition,
            envelope.offset,
            envelope.payloadSha256,
            envelope.schemaId,
            envelope.timestampMs,
            envelope.keyId,
        ).joinToString("|")

        val signatureBytes = try {
            Base64.getDecoder().decode(envelope.signature)
        } catch (_: IllegalArgumentException) {
            return VerificationResult.failed()
        }

        val verified = try {
            val sig = Signature.getInstance("Ed25519")
            sig.initVerify(publicKey)
            sig.update(canonical.toByteArray(StandardCharsets.UTF_8))
            sig.verify(signatureBytes)
        } catch (_: NoSuchAlgorithmException) {
            false
        } catch (_: InvalidKeyException) {
            false
        } catch (_: SignatureException) {
            false
        }

        return VerificationResult(
            verified = verified,
            producerId = envelope.keyId,
            schemaId = if (envelope.schemaId != 0) envelope.schemaId else null,
            contractId = envelope.contractId,
            timestampMs = envelope.timestampMs,
        )
    }

    companion object {
        /** Kafka header name carrying the attestation envelope. */
        const val ATTEST_HEADER = "streamline-attest"
    }
}
