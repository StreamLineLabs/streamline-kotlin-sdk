package io.streamline.sdk

/**
 * TLS configuration for encrypted connections to a Streamline server.
 *
 * @property enabled Whether TLS is enabled.
 * @property trustStorePath Path to the JKS/PKCS12 trust store file.
 * @property trustStorePassword Password for the trust store.
 * @property keyStorePath Path to the JKS/PKCS12 key store file (mutual TLS).
 * @property keyStorePassword Password for the key store.
 * @property insecureSkipVerify Skip server certificate verification (development only).
 */
data class TlsConfig(
    val enabled: Boolean = false,
    val trustStorePath: String? = null,
    val trustStorePassword: String? = null,
    val keyStorePath: String? = null,
    val keyStorePassword: String? = null,
    val insecureSkipVerify: Boolean = false,
)

/** Supported SASL authentication mechanisms. */
enum class SaslMechanism { PLAIN, SCRAM_SHA_256, SCRAM_SHA_512 }

/**
 * SASL authentication configuration for connecting to a secured Streamline server.
 *
 * @property mechanism The SASL mechanism to use.
 * @property username Authentication username.
 * @property password Authentication password.
 */
data class SaslConfig(
    val mechanism: SaslMechanism = SaslMechanism.PLAIN,
    val username: String,
    val password: String,
)
