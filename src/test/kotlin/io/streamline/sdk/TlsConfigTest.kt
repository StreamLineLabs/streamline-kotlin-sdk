package io.streamline.sdk

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

class TlsConfigTest {

    // -- TlsConfig construction --

    @Test
    fun `default TlsConfig is disabled`() {
        val tls = TlsConfig()
        assertFalse(tls.enabled)
        assertNull(tls.trustStorePath)
        assertNull(tls.trustStorePassword)
        assertNull(tls.keyStorePath)
        assertNull(tls.keyStorePassword)
        assertFalse(tls.insecureSkipVerify)
    }

    @Test
    fun `TlsConfig stores all fields`() {
        val tls = TlsConfig(
            enabled = true,
            trustStorePath = "/path/to/truststore.jks",
            trustStorePassword = "ts-pass",
            keyStorePath = "/path/to/keystore.jks",
            keyStorePassword = "ks-pass",
            insecureSkipVerify = false,
        )
        assertTrue(tls.enabled)
        assertEquals("/path/to/truststore.jks", tls.trustStorePath)
        assertEquals("ts-pass", tls.trustStorePassword)
        assertEquals("/path/to/keystore.jks", tls.keyStorePath)
        assertEquals("ks-pass", tls.keyStorePassword)
        assertFalse(tls.insecureSkipVerify)
    }

    // -- TlsConfig validation --

    @Test
    fun `disabled TlsConfig passes validation`() {
        TlsConfig(enabled = false).validate()
    }

    @Test
    fun `enabled TlsConfig with no stores passes validation`() {
        TlsConfig(enabled = true).validate()
    }

    @Test
    fun `enabled TlsConfig with complete trust store passes validation`() {
        TlsConfig(
            enabled = true,
            trustStorePath = "/trust.jks",
            trustStorePassword = "pass",
        ).validate()
    }

    @Test
    fun `enabled TlsConfig with complete key store passes validation`() {
        TlsConfig(
            enabled = true,
            keyStorePath = "/key.jks",
            keyStorePassword = "pass",
        ).validate()
    }

    @Test
    fun `enabled TlsConfig with full mTLS config passes validation`() {
        TlsConfig(
            enabled = true,
            trustStorePath = "/trust.jks",
            trustStorePassword = "tp",
            keyStorePath = "/key.jks",
            keyStorePassword = "kp",
        ).validate()
    }

    @Test
    fun `keyStorePath without keyStorePassword fails validation`() {
        val tls = TlsConfig(
            enabled = true,
            keyStorePath = "/key.jks",
            keyStorePassword = null,
        )
        val ex = assertFailsWith<ConfigurationException> { tls.validate() }
        assertTrue(ex.message!!.contains("keyStorePassword"))
    }

    @Test
    fun `trustStorePath without trustStorePassword fails validation`() {
        val tls = TlsConfig(
            enabled = true,
            trustStorePath = "/trust.jks",
            trustStorePassword = null,
        )
        val ex = assertFailsWith<ConfigurationException> { tls.validate() }
        assertTrue(ex.message!!.contains("trustStorePassword"))
    }

    // -- StreamlineConfiguration wiring --

    @Test
    fun `StreamlineConfiguration accepts TlsConfig`() {
        val tls = TlsConfig(enabled = true, insecureSkipVerify = true)
        val config = StreamlineConfiguration(url = "wss://localhost:9092", tls = tls)
        assertEquals(tls, config.tls)
        assertTrue(config.tls!!.enabled)
    }

    @Test
    fun `StreamlineConfiguration defaults tls to null`() {
        val config = StreamlineConfiguration(url = "ws://localhost:9092")
        assertNull(config.tls)
    }

    // -- Serializer interface --

    @Test
    fun `StringSerializer passes through values`() {
        val serializer = StringSerializer()
        val payload = """{"key":"value"}"""
        assertEquals(payload, serializer.serialize("test-topic", payload))
        assertEquals(payload, serializer.deserialize("test-topic", payload))
    }

    @Test
    fun `StringSerializer handles empty strings`() {
        val serializer = StringSerializer()
        assertEquals("", serializer.serialize("t", ""))
        assertEquals("", serializer.deserialize("t", ""))
    }

    @Test
    fun `custom MessageSerializer implementation works`() {
        val intSerializer = object : MessageSerializer<Int> {
            override fun serialize(topic: String, value: Int): String = value.toString()
            override fun deserialize(topic: String, data: String): Int = data.toInt()
        }
        assertEquals("42", intSerializer.serialize("numbers", 42))
        assertEquals(42, intSerializer.deserialize("numbers", "42"))
    }

    @Test
    fun `custom MessageSerializer can use topic for routing`() {
        val prefixSerializer = object : MessageSerializer<String> {
            override fun serialize(topic: String, value: String): String = "[$topic] $value"
            override fun deserialize(topic: String, data: String): String =
                data.removePrefix("[$topic] ")
        }
        assertEquals("[events] hello", prefixSerializer.serialize("events", "hello"))
        assertEquals("hello", prefixSerializer.deserialize("events", "[events] hello"))
    }

    // -- AdminClient TLS parameter --

    @Test
    fun `AdminClient accepts tls parameter`() {
        val tls = TlsConfig(enabled = false)
        val admin = AdminClient(baseUrl = "http://localhost:9094", tls = tls)
        admin.close()
    }

    @Test
    fun `AdminClient defaults tls to null`() {
        val admin = AdminClient(baseUrl = "http://localhost:9094")
        admin.close()
    }
}
