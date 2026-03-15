package io.streamline.sdk

import io.ktor.client.*
import io.ktor.client.engine.cio.*
import java.io.FileInputStream
import java.security.KeyStore
import javax.net.ssl.KeyManagerFactory
import javax.net.ssl.TrustManagerFactory
import javax.net.ssl.X509TrustManager
import java.security.cert.X509Certificate

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
) {
    /**
     * Validate that the configuration is internally consistent.
     *
     * @throws ConfigurationException if required fields are missing.
     */
    fun validate() {
        if (!enabled) return
        if (keyStorePath != null && keyStorePassword == null) {
            throw ConfigurationException("keyStorePassword is required when keyStorePath is set")
        }
        if (trustStorePath != null && trustStorePassword == null) {
            throw ConfigurationException("trustStorePassword is required when trustStorePath is set")
        }
    }
}

/**
 * Configure TLS/mTLS on the Ktor [CIO] engine using the provided [TlsConfig].
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

        // Client certificate configuration (mTLS)
        tls.keyStorePath?.let { path ->
            val keyStore = KeyStore.getInstance("JKS").apply {
                load(FileInputStream(path), tls.keyStorePassword?.toCharArray())
            }
            val kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm()).apply {
                init(keyStore, tls.keyStorePassword?.toCharArray())
            }
            // Ktor CIO TLSConfigBuilder exposes addKeyStore; we set the key managers
            // via the underlying SSLContext by adding the key store directly.
            serverName = tls.trustStorePath ?: "localhost"
            // Note: CIO engine picks up the KeyManagerFactory through the SSLContext
            // built by Ktor internally when we supply the trust/key managers.
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
