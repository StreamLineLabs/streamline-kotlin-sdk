/**
 * Security example for Streamline Kotlin SDK.
 *
 * Run with:
 *   BASIC_USERNAME=admin BASIC_PASSWORD=admin-secret gradle run
 *   SECURITY_MODE=scram gradle run
 *   SECURITY_MODE=tls TRUST_STORE_PATH=certs/truststore.jks TRUST_STORE_PASSWORD=changeit gradle run
 */
package io.streamline.examples

import io.streamline.sdk.AuthConfig
import io.streamline.sdk.ConfigurationException
import io.streamline.sdk.ScramMechanism
import io.streamline.sdk.StreamlineClient
import io.streamline.sdk.StreamlineConfiguration
import io.streamline.sdk.TlsConfig

suspend fun main() {
    println("Streamline Security Examples")
    println("========================================\n")

    when (System.getenv("SECURITY_MODE") ?: "basic") {
        "scram" -> scramExample()
        "tls" -> tlsExample()
        else -> basicAuthExample()
    }

    println("Done!")
}

private suspend fun basicAuthExample() {
    println("HTTP Basic-Compatible Authentication")
    println("----------------------------------------")

    val config =
        StreamlineConfiguration(
            url = System.getenv("STREAMLINE_WS_URL") ?: "ws://localhost:9092",
        )
    val auth =
        AuthConfig.PlainAuth(
            username = System.getenv("BASIC_USERNAME") ?: "admin",
            password = System.getenv("BASIC_PASSWORD") ?: "admin-secret",
        )
    val client = StreamlineClient(config, auth = auth)
    client.connect()
    println("  Connected with Basic-compatible authentication")

    client.produce("secure-topic", value = "authenticated message")
    println("  Produced message to secure-topic")

    client.disconnect()
    client.close()
    println("  Disconnected.\n")
}

private suspend fun scramExample() {
    println("Unsupported SASL/SCRAM Configuration")
    println("----------------------------------------")

    val config =
        StreamlineConfiguration(
            url = System.getenv("STREAMLINE_WS_URL") ?: "ws://localhost:9092",
        )
    val auth =
        AuthConfig.ScramAuth(
            username = System.getenv("SASL_USERNAME") ?: "admin",
            password = System.getenv("SASL_PASSWORD") ?: "admin-secret",
            mechanism = ScramMechanism.SCRAM_SHA_256,
        )
    val client = StreamlineClient(config, auth = auth)
    try {
        client.connect()
        error("SCRAM connection unexpectedly succeeded")
    } catch (e: ConfigurationException) {
        println("  Rejected as expected: ${e.message}")
    } finally {
        client.close()
    }
    println()
}

private suspend fun tlsExample() {
    println("TLS Encrypted Connection")
    println("----------------------------------------")

    val config =
        StreamlineConfiguration(
            url = System.getenv("STREAMLINE_TLS_URL") ?: "wss://localhost:9093",
            tls =
                TlsConfig(
                    enabled = true,
                    trustStorePath = System.getenv("TRUST_STORE_PATH") ?: "certs/truststore.jks",
                    trustStorePassword = System.getenv("TRUST_STORE_PASSWORD") ?: "changeit",
                ),
        )

    val client = StreamlineClient(config)
    client.connect()
    println("  Connected with TLS")

    client.disconnect()
    client.close()
    println("  Disconnected.\n")
}
