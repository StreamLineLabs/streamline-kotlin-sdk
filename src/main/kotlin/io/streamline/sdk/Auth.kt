package io.streamline.sdk

import io.ktor.client.request.*
import io.ktor.http.*
import java.util.Base64

/**
 * Sealed hierarchy for authentication configuration.
 *
 * ```kotlin
 * val auth = AuthConfig.ScramAuth("admin", "secret", ScramMechanism.SCRAM_SHA_256)
 * val client = StreamlineClient(config, auth = auth)
 * ```
 */
sealed class AuthConfig {

    /** SASL/PLAIN authentication using username and password. */
    data class PlainAuth(val username: String, val password: String) : AuthConfig() {
        init {
            require(username.isNotBlank()) { "Username must not be blank" }
            require(password.isNotBlank()) { "Password must not be blank" }
        }
    }

    /** SASL/SCRAM authentication using username, password, and mechanism. */
    data class ScramAuth(
        val username: String,
        val password: String,
        val mechanism: ScramMechanism = ScramMechanism.SCRAM_SHA_256,
    ) : AuthConfig() {
        init {
            require(username.isNotBlank()) { "Username must not be blank" }
            require(password.isNotBlank()) { "Password must not be blank" }
        }
    }

    /** OAuth 2.0 bearer-token authentication with a refreshable token provider. */
    data class OAuthBearerAuth(val tokenProvider: suspend () -> OAuthToken) : AuthConfig()
}

/** SCRAM mechanism variants supported by Streamline. */
enum class ScramMechanism {
    SCRAM_SHA_256,
    SCRAM_SHA_512,
}

/** An OAuth bearer token with its expiration timestamp. */
data class OAuthToken(
    val token: String,
    val expiresAtMs: Long,
) {
    /** Whether this token has expired. */
    fun isExpired(): Boolean = System.currentTimeMillis() >= expiresAtMs
}

/**
 * Applies the given [AuthConfig] to an HTTP request builder by setting
 * the appropriate `Authorization` header.
 */
internal suspend fun HttpRequestBuilder.applyAuth(auth: AuthConfig?) {
    when (auth) {
        is AuthConfig.PlainAuth -> {
            val credentials = Base64.getEncoder()
                .encodeToString("${auth.username}:${auth.password}".toByteArray())
            header(HttpHeaders.Authorization, "Basic $credentials")
        }
        is AuthConfig.ScramAuth -> {
            val credentials = Base64.getEncoder()
                .encodeToString("${auth.username}:${auth.password}".toByteArray())
            header(HttpHeaders.Authorization, "SCRAM ${auth.mechanism.name} $credentials")
        }
        is AuthConfig.OAuthBearerAuth -> {
            val oauthToken = auth.tokenProvider()
            header(HttpHeaders.Authorization, "Bearer ${oauthToken.token}")
        }
        null -> { /* no auth */ }
    }
}

/**
 * Returns a map of WebSocket connection headers for the given [AuthConfig].
 */
internal suspend fun authHeaders(auth: AuthConfig?): Map<String, String> {
    return when (auth) {
        is AuthConfig.PlainAuth -> {
            val credentials = Base64.getEncoder()
                .encodeToString("${auth.username}:${auth.password}".toByteArray())
            mapOf("Authorization" to "Basic $credentials")
        }
        is AuthConfig.ScramAuth -> {
            val credentials = Base64.getEncoder()
                .encodeToString("${auth.username}:${auth.password}".toByteArray())
            mapOf("Authorization" to "SCRAM ${auth.mechanism.name} $credentials")
        }
        is AuthConfig.OAuthBearerAuth -> {
            val oauthToken = auth.tokenProvider()
            mapOf("Authorization" to "Bearer ${oauthToken.token}")
        }
        null -> emptyMap()
    }
}
