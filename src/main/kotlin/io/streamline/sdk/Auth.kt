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

    /** HTTP Basic-compatible authentication using username and password. */
    data class PlainAuth(val username: String, val password: String) : AuthConfig() {
        init {
            require(username.isNotBlank()) { "Username must not be blank" }
            require(password.isNotBlank()) { "Password must not be blank" }
        }

        override fun toString(): String =
            "PlainAuth(username=$username, password=[REDACTED])"
    }

    /**
     * Retained for source compatibility. SCRAM negotiation is not implemented
     * by the current HTTP/WebSocket transports and use is rejected explicitly.
     */
    data class ScramAuth(
        val username: String,
        val password: String,
        val mechanism: ScramMechanism = ScramMechanism.SCRAM_SHA_256,
    ) : AuthConfig() {
        init {
            require(username.isNotBlank()) { "Username must not be blank" }
            require(password.isNotBlank()) { "Password must not be blank" }
        }

        override fun toString(): String =
            "ScramAuth(username=$username, password=[REDACTED], mechanism=$mechanism)"
    }

    /** OAuth 2.0 bearer-token authentication with a refreshable token provider. */
    data class OAuthBearerAuth(val tokenProvider: suspend () -> OAuthToken) : AuthConfig() {
        override fun toString(): String =
            "OAuthBearerAuth(tokenProvider=[REDACTED])"
    }
}

/** SCRAM mechanism identifiers retained for source compatibility. */
enum class ScramMechanism {
    SCRAM_SHA_256,
    SCRAM_SHA_512,
}

/** An OAuth bearer token with its expiration timestamp. */
data class OAuthToken(
    val token: String,
    val expiresAtMs: Long,
) {
    init {
        require(token.isNotBlank()) { "Token must not be blank" }
    }

    /** Whether this token has expired. */
    fun isExpired(): Boolean = System.currentTimeMillis() >= expiresAtMs

    override fun toString(): String =
        "OAuthToken(token=[REDACTED], expiresAtMs=$expiresAtMs)"
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
            throw unsupportedScram()
        }
        is AuthConfig.OAuthBearerAuth -> {
            val oauthToken = auth.tokenProvider()
            if (oauthToken.isExpired()) {
                throw AuthenticationFailedException("OAuth bearer token is expired")
            }
            header(HttpHeaders.Authorization, bearerAuthorization(oauthToken.token))
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
            throw unsupportedScram()
        }
        is AuthConfig.OAuthBearerAuth -> {
            val oauthToken = auth.tokenProvider()
            if (oauthToken.isExpired()) {
                throw AuthenticationFailedException("OAuth bearer token is expired")
            }
            mapOf("Authorization" to bearerAuthorization(oauthToken.token))
        }
        null -> emptyMap()
    }
}

internal fun bearerAuthorization(token: String): String {
    if (token.isBlank()) {
        throw ConfigurationException("Bearer auth token must not be blank")
    }
    return "Bearer $token"
}

private fun unsupportedScram(): ConfigurationException =
    ConfigurationException(
        "SCRAM authentication is not implemented by the current HTTP/WebSocket transport"
    )
