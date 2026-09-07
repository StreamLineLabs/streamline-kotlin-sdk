package io.streamline.sdk

import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import java.nio.charset.StandardCharsets
import java.security.KeyPair
import java.security.KeyPairGenerator
import java.security.MessageDigest
import java.security.Signature
import java.util.Base64
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

@Suppress("DEPRECATION") // exercises the legacy single-key compatibility constructor intentionally
class StreamlineVerifierTest {
    private val keyPair: KeyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair()
    private val verifier = StreamlineVerifier(keyPair.public, expectedKeyId = "producer-key-1")
    private val json = Json

    @Test
    fun `valid attestation is bound to message contents and location`() {
        val message =
            attestedMessage(
                topic = "orders",
                partition = 2,
                offset = 41,
                value = """{"id":"order-1"}""",
            )

        assertTrue(verifier.verify(message).verified)
    }

    @Test
    fun `payload tampering invalidates otherwise valid attestation`() {
        val message =
            attestedMessage(
                topic = "orders",
                partition = 2,
                offset = 41,
                value = """{"amount":100}""",
            )

        assertFalse(verifier.verify(message.copy(value = """{"amount":900}""")).verified)
    }

    @Test
    fun `attestation cannot be substituted onto another topic or partition`() {
        val message =
            attestedMessage(
                topic = "orders",
                partition = 2,
                offset = 41,
                value = "payload",
            )

        assertFalse(verifier.verify(message.copy(topic = "refunds")).verified)
        assertFalse(verifier.verify(message.copy(partition = 3)).verified)
    }

    @Test
    fun `attestation replay at another offset is rejected`() {
        val message =
            attestedMessage(
                topic = "orders",
                partition = 2,
                offset = 41,
                value = "payload",
            )

        assertFalse(verifier.verify(message.copy(offset = 42)).verified)
    }

    @Test
    fun `attestation requires concrete partition and offset metadata`() {
        val message =
            attestedMessage(
                topic = "orders",
                partition = 2,
                offset = 41,
                value = "payload",
            )

        assertFalse(verifier.verify(message.copy(partition = null)).verified)
        assertFalse(verifier.verify(message.copy(offset = null)).verified)
    }

    // -- key_id trust binding --

    private val otherKeyPair: KeyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair()

    @Test
    fun `legacy single key constructor fails closed without trusted identity`() {
        val legacyVerifier = StreamlineVerifier(keyPair.public)
        val message = attestedMessage(topic = "orders", partition = 2, offset = 41, value = "payload")

        assertEquals(VerificationResult(verified = false), legacyVerifier.verify(message))
    }

    @Test
    fun `expected key id constructor accepts the matching identity`() {
        val boundVerifier = StreamlineVerifier(keyPair.public, expectedKeyId = "producer-key-1")
        val message = attestedMessage(topic = "orders", partition = 2, offset = 41, value = "payload")

        val result = boundVerifier.verify(message)

        assertTrue(result.verified)
        assertEquals("producer-key-1", result.producerId)
    }

    @Test
    fun `expected key id constructor rejects a relabeled identity fail-closed`() {
        // Same signer, same signature machinery, but the envelope claims a
        // different key_id than the one this verifier trusts. A valid
        // signature alone must not be enough to authenticate the identity.
        val boundVerifier = StreamlineVerifier(keyPair.public, expectedKeyId = "producer-key-1")
        val message =
            attestedMessage(
                topic = "orders",
                partition = 2,
                offset = 41,
                value = "payload",
                keyId = "producer-key-9",
                signer = keyPair,
            )

        val result = boundVerifier.verify(message)

        assertFalse(result.verified)
        assertEquals(VerificationResult(verified = false), result)
    }

    @Test
    fun `trusted resolver rejects a signature from an untrusted key impersonating another producer`() {
        // Attacker controls otherKeyPair but has no key registered for
        // "producer-A". Claiming that key_id must not let the resolver fall
        // back to verifying against whatever key actually signed it.
        val resolver =
            TrustedKeyResolver { keyId ->
                when (keyId) {
                    "producer-A" -> keyPair.public
                    "producer-B" -> otherKeyPair.public
                    else -> null
                }
            }
        val boundVerifier = StreamlineVerifier(resolver)
        val message =
            attestedMessage(
                topic = "orders",
                partition = 2,
                offset = 41,
                value = "payload",
                keyId = "producer-A",
                signer = otherKeyPair,
            )

        val result = boundVerifier.verify(message)

        assertFalse(result.verified)
    }

    @Test
    fun `trusted resolver accepts each producer signing under its own registered key`() {
        val resolver =
            TrustedKeyResolver { keyId ->
                when (keyId) {
                    "producer-A" -> keyPair.public
                    "producer-B" -> otherKeyPair.public
                    else -> null
                }
            }
        val boundVerifier = StreamlineVerifier(resolver)

        val fromA =
            attestedMessage(
                topic = "orders",
                partition = 2,
                offset = 41,
                value = "a",
                keyId = "producer-A",
                signer = keyPair,
            )
        val fromB =
            attestedMessage(
                topic = "orders",
                partition = 2,
                offset = 42,
                value = "b",
                keyId = "producer-B",
                signer = otherKeyPair,
            )

        assertTrue(boundVerifier.verify(fromA).verified)
        assertTrue(boundVerifier.verify(fromB).verified)
    }

    @Test
    fun `trusted resolver rejects an unknown key id without leaking the claimed producer`() {
        val resolver = TrustedKeyResolver { keyId -> if (keyId == "producer-A") keyPair.public else null }
        val boundVerifier = StreamlineVerifier(resolver)
        val message =
            attestedMessage(
                topic = "orders",
                partition = 2,
                offset = 41,
                value = "payload",
                keyId = "unregistered-producer",
                signer = keyPair,
            )

        val result = boundVerifier.verify(message)

        assertEquals(VerificationResult(verified = false), result)
    }

    private fun attestedMessage(
        topic: String,
        partition: Int,
        offset: Long,
        value: String,
        keyId: String = "producer-key-1",
        signer: KeyPair = keyPair,
    ): StreamlineMessage {
        val payloadHash =
            MessageDigest.getInstance("SHA-256")
                .digest(value.toByteArray(StandardCharsets.UTF_8))
                .joinToString("") { "%02x".format(it.toInt() and 0xff) }
        val unsignedEnvelope =
            AttestationEnvelope(
                payloadSha256 = payloadHash,
                topic = topic,
                partition = partition,
                offset = offset,
                schemaId = 7,
                timestampMs = 1_725_000_000_000,
                keyId = keyId,
                signature = "",
                contractId = "orders-v1",
            )
        val canonical =
            listOf(
                unsignedEnvelope.topic,
                unsignedEnvelope.partition,
                unsignedEnvelope.offset,
                unsignedEnvelope.payloadSha256,
                unsignedEnvelope.schemaId,
                unsignedEnvelope.timestampMs,
                unsignedEnvelope.keyId,
            ).joinToString("|")
        val signature =
            Signature.getInstance("Ed25519").run {
                initSign(signer.private)
                update(canonical.toByteArray(StandardCharsets.UTF_8))
                Base64.getEncoder().encodeToString(sign())
            }
        val envelope = unsignedEnvelope.copy(signature = signature)
        val header =
            Base64.getEncoder().encodeToString(
                json.encodeToString(envelope).toByteArray(StandardCharsets.UTF_8),
            )
        return StreamlineMessage(
            topic = topic,
            value = value,
            partition = partition,
            offset = offset,
            headers = mapOf(StreamlineVerifier.ATTEST_HEADER to header),
        )
    }
}
