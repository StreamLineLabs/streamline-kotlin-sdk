package io.streamline.sdk

import io.ktor.client.*
import io.ktor.client.engine.cio.*
import java.io.FileInputStream
import java.security.KeyStore
import javax.net.ssl.TrustManagerFactory
import javax.net.ssl.X509TrustManager
import java.security.cert.X509Certificate

/**
 * TLS configuration for encrypted connections to a Streamline server.
 *
 * @property enabled Whether TLS is enabled.
 * @property trustStorePath Path to the JKS trust store file.
 * @property trustStorePassword Password for the trust store.
 * @property keyStorePath Retained for source compatibility; currently rejected.
 * @property keyStorePassword Retained for source compatibility; currently rejected.
 * @property insecureSkipVerify Skip server certificate verification (development only).
 */
data class TlsConfig(
    val enabled: Boolean = false,
    val trustStorePath: String? = null,
    val trustStorePassword: String? = null,
    val keyStorePath: String? = null,
    val keyStorePassword: String? = null,
    val insecureSkipVerify: Boolean = false,
) {
    /**
     * Validate that the configuration is internally consistent.
     *
     * @throws ConfigurationException if required fields are missing or an
     * unsupported client-certificate option is configured.
     */
    fun validate() {
        if (keyStorePath != null || keyStorePassword != null) {
            throw ConfigurationException(
                "Client certificate authentication (mTLS) is not implemented by the CIO transport"
            )
        }
        if (!enabled) return
        if (trustStorePath != null && trustStorePassword == null) {
            throw ConfigurationException("trustStorePassword is required when trustStorePath is set")
        }
        if (trustStorePath == null && trustStorePassword != null) {
            throw ConfigurationException("trustStorePath is required when trustStorePassword is set")
        }
    }

    override fun toString(): String =
        "TlsConfig(" +
            "enabled=$enabled, " +
            "trustStorePath=$trustStorePath, " +
            "trustStorePassword=${trustStorePassword?.let { "[REDACTED]" }}, " +
            "keyStorePath=$keyStorePath, " +
            "keyStorePassword=${keyStorePassword?.let { "[REDACTED]" }}, " +
            "insecureSkipVerify=$insecureSkipVerify" +
            ")"
}

/**
 * Configure supported TLS options on the Ktor [CIO] engine using [TlsConfig].
 *
 * This is the single point where SSL settings are wired into Ktor.  Both
 * [StreamlineClient] and [AdminClient] call this when building their
 * [HttpClient] instances.
 */
internal fun CIOEngineConfig.configureTls(tls: TlsConfig?) {
    if (tls == null || !tls.enabled) return
    tls.validate()

    https {
        // Trust store configuration
        tls.trustStorePath?.let { path ->
            val trustStore = KeyStore.getInstance("JKS").apply {
                load(FileInputStream(path), tls.trustStorePassword?.toCharArray())
            }
            val tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm()).apply {
                init(trustStore)
            }
            trustManager = tmf.trustManagers.first() as X509TrustManager
        }

        // Insecure mode — development only
        if (tls.insecureSkipVerify) {
            @Suppress("CustomX509TrustManager")
            trustManager = object : X509TrustManager {
                @Suppress("TrustAllX509TrustManager")
                override fun checkClientTrusted(chain: Array<out X509Certificate>?, authType: String?) {}
                @Suppress("TrustAllX509TrustManager")
                override fun checkServerTrusted(chain: Array<out X509Certificate>?, authType: String?) {}
                override fun getAcceptedIssuers(): Array<X509Certificate> = arrayOf()
            }
        }
    }
}

/** Legacy SASL mechanism identifiers retained for source compatibility. */
enum class SaslMechanism { PLAIN, SCRAM_SHA_256, SCRAM_SHA_512 }

/**
 * Legacy SASL authentication configuration. Passing this to
 * [StreamlineConfiguration] is rejected because the transport is not wired.
 *
 * @property mechanism The SASL mechanism to use.
 * @property username Authentication username.
 * @property password Authentication password.
 */
data class SaslConfig(
    val mechanism: SaslMechanism = SaslMechanism.PLAIN,
    val username: String,
    val password: String,
) {
    init {
        require(username.isNotBlank()) { "Username must not be blank" }
        require(password.isNotBlank()) { "Password must not be blank" }
    }

    override fun toString(): String =
        "SaslConfig(mechanism=$mechanism, username=$username, password=[REDACTED])"
}
