/**
 * Security example for Streamline Kotlin SDK.
 *
 * Prerequisites:
 *   1. Start a Streamline server with auth enabled
 *   2. Set environment variables as shown below
 *
 * Run with:
 *   SASL_USERNAME=admin SASL_PASSWORD=admin-secret gradle run
 *   SECURITY_MODE=scram SASL_USERNAME=admin SASL_PASSWORD=admin-secret gradle run
 *   SECURITY_MODE=tls CA_PATH=certs/ca.pem gradle run
 */
package io.streamline.examples

import io.streamline.sdk.*

suspend fun main() {
    println("Streamline Security Examples")
    println("========================================\n")

    val mode = System.getenv("SECURITY_MODE") ?: "sasl_plain"

    when (mode) {
        "scram" -> scramExample()
        "tls" -> tlsExample()
        else -> saslPlainExample()
    }

    println("Done!")
}

private suspend fun saslPlainExample() {
    println("SASL/PLAIN Authentication")
    println("----------------------------------------")

    val config = StreamlineConfig(
        url = System.getenv("STREAMLINE_WS_URL") ?: "ws://localhost:9092",
        authConfig = AuthConfig.PlainAuth(
            username = System.getenv("SASL_USERNAME") ?: "admin",
            password = System.getenv("SASL_PASSWORD") ?: "admin-secret",
        ),
    )

    val client = StreamlineClient(config)
    client.connect()
    println("  Connected with SASL/PLAIN")

    client.produce("secure-topic", value = "authenticated message")
    println("  Produced message to secure-topic")

    client.disconnect()
    println("  Disconnected.\n")
}

private suspend fun scramExample() {
    println("SASL/SCRAM-SHA-256 Authentication")
    println("----------------------------------------")

    val config = StreamlineConfig(
        url = System.getenv("STREAMLINE_WS_URL") ?: "ws://localhost:9092",
        authConfig = AuthConfig.ScramAuth(
            username = System.getenv("SASL_USERNAME") ?: "admin",
            password = System.getenv("SASL_PASSWORD") ?: "admin-secret",
            mechanism = ScramMechanism.SCRAM_SHA_256,
        ),
    )

    val client = StreamlineClient(config)
    client.connect()
    println("  Connected with SCRAM-SHA-256")

    client.disconnect()
    println("  Disconnected.\n")
}

private suspend fun tlsExample() {
    println("TLS Encrypted Connection")
    println("----------------------------------------")

    val config = StreamlineConfig(
        url = System.getenv("STREAMLINE_TLS_URL") ?: "wss://localhost:9093",
        tls = TlsConfig(
            caPath = System.getenv("CA_PATH") ?: "certs/ca.pem",
            certPath = System.getenv("CLIENT_CERT_PATH"),
            keyPath = System.getenv("CLIENT_KEY_PATH"),
        ),
    )

    val client = StreamlineClient(config)
    client.connect()
    println("  Connected with TLS")

    client.disconnect()
    println("  Disconnected.\n")
}
