package io.streamline.sdk

import io.ktor.client.*
import io.ktor.client.engine.mock.*
import io.ktor.client.plugins.contentnegotiation.*
import io.ktor.client.request.*
import io.ktor.http.*
import io.ktor.serialization.kotlinx.json.*
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.json.Json
import java.util.Base64
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class AuthTest {

    private fun mockClient(handler: suspend MockRequestHandleScope.(HttpRequestData) -> HttpResponseData): HttpClient {
        return HttpClient(MockEngine) {
            engine {
                addHandler { request -> handler(request) }
            }
            install(ContentNegotiation) {
                json(Json { ignoreUnknownKeys = true; encodeDefaults = true })
            }
        }
    }

    // -- AuthConfig construction and validation --

    @Test
    fun `PlainAuth stores credentials`() {
        val auth = AuthConfig.PlainAuth("admin", "secret123")
        assertEquals("admin", auth.username)
        assertEquals("secret123", auth.password)
    }

    @Test
    fun `PlainAuth rejects blank username`() {
        assertFailsWith<IllegalArgumentException> {
            AuthConfig.PlainAuth("", "password")
        }
    }

    @Test
    fun `PlainAuth rejects blank password`() {
        assertFailsWith<IllegalArgumentException> {
            AuthConfig.PlainAuth("user", "")
        }
    }

    @Test
    fun `ScramAuth stores credentials and mechanism`() {
        val auth = AuthConfig.ScramAuth("admin", "secret", ScramMechanism.SCRAM_SHA_256)
        assertEquals("admin", auth.username)
        assertEquals("secret", auth.password)
        assertEquals(ScramMechanism.SCRAM_SHA_256, auth.mechanism)
    }

    @Test
    fun `ScramAuth defaults to SHA-256`() {
        val auth = AuthConfig.ScramAuth("user", "pass")
        assertEquals(ScramMechanism.SCRAM_SHA_256, auth.mechanism)
    }

    @Test
    fun `ScramAuth rejects blank username`() {
        assertFailsWith<IllegalArgumentException> {
            AuthConfig.ScramAuth("", "password")
        }
    }

    @Test
    fun `ScramAuth rejects blank password`() {
        assertFailsWith<IllegalArgumentException> {
            AuthConfig.ScramAuth("user", "")
        }
    }

    @Test
    fun `OAuthBearerAuth stores token provider`() {
        val auth = AuthConfig.OAuthBearerAuth { OAuthToken("tok", 999L) }
        assertNotNull(auth.tokenProvider)
    }

    @Test
    fun `OAuthToken expiration check`() {
        val expired = OAuthToken("tok", System.currentTimeMillis() - 1000)
        assertTrue(expired.isExpired())

        val valid = OAuthToken("tok", System.currentTimeMillis() + 60_000)
        assertFalse(valid.isExpired())
    }

    // -- ScramMechanism enum --

    @Test
    fun `ScramMechanism has expected values`() {
        val mechanisms = ScramMechanism.entries
        assertEquals(2, mechanisms.size)
        assertTrue(mechanisms.contains(ScramMechanism.SCRAM_SHA_256))
        assertTrue(mechanisms.contains(ScramMechanism.SCRAM_SHA_512))
    }

    // -- AuthConfig is sealed class --

    @Test
    fun `AuthConfig subtypes are distinguishable`() {
        val plain: AuthConfig = AuthConfig.PlainAuth("u", "p")
        val scram: AuthConfig = AuthConfig.ScramAuth("u", "p", ScramMechanism.SCRAM_SHA_512)
        val oauth: AuthConfig = AuthConfig.OAuthBearerAuth { OAuthToken("t", 0L) }

        assertTrue(plain is AuthConfig.PlainAuth)
        assertTrue(scram is AuthConfig.ScramAuth)
        assertTrue(oauth is AuthConfig.OAuthBearerAuth)
    }

    // -- AdminClient auth integration --

    @Test
    fun `AdminClient with PlainAuth sends Basic header`() = runTest {
        var capturedAuth: String? = null
        val http = mockClient { request ->
            capturedAuth = request.headers[HttpHeaders.Authorization]
            respond(
                content = "[]",
                headers = headersOf(HttpHeaders.ContentType, "application/json"),
            )
        }

        val admin = AdminClient(
            "http://localhost:9094",
            auth = AuthConfig.PlainAuth("admin", "secret"),
            httpClient = http,
        )
        admin.listTopics()

        assertNotNull(capturedAuth)
        assertTrue(capturedAuth!!.startsWith("Basic "))
        val decoded = String(Base64.getDecoder().decode(capturedAuth!!.removePrefix("Basic ")))
        assertEquals("admin:secret", decoded)
        admin.close()
    }

    @Test
    fun `AdminClient with ScramAuth sends SCRAM header`() = runTest {
        var capturedAuth: String? = null
        val http = mockClient { request ->
            capturedAuth = request.headers[HttpHeaders.Authorization]
            respond(
                content = "[]",
                headers = headersOf(HttpHeaders.ContentType, "application/json"),
            )
        }

        val admin = AdminClient(
            "http://localhost:9094",
            auth = AuthConfig.ScramAuth("admin", "secret", ScramMechanism.SCRAM_SHA_512),
            httpClient = http,
        )
        admin.listTopics()

        assertNotNull(capturedAuth)
        assertTrue(capturedAuth!!.startsWith("SCRAM SCRAM_SHA_512 "))
        val encoded = capturedAuth!!.removePrefix("SCRAM SCRAM_SHA_512 ")
        val decoded = String(Base64.getDecoder().decode(encoded))
        assertEquals("admin:secret", decoded)
        admin.close()
    }

    @Test
    fun `AdminClient with OAuthBearerAuth sends Bearer header`() = runTest {
        var capturedAuth: String? = null
        val http = mockClient { request ->
            capturedAuth = request.headers[HttpHeaders.Authorization]
            respond(
                content = "[]",
                headers = headersOf(HttpHeaders.ContentType, "application/json"),
            )
        }

        val admin = AdminClient(
            "http://localhost:9094",
            auth = AuthConfig.OAuthBearerAuth {
                OAuthToken("my-oauth-token", System.currentTimeMillis() + 3600_000)
            },
            httpClient = http,
        )
        admin.listTopics()

        assertEquals("Bearer my-oauth-token", capturedAuth)
        admin.close()
    }

    @Test
    fun `AdminClient auth config takes precedence over legacy authToken`() = runTest {
        var capturedAuth: String? = null
        val http = mockClient { request ->
            capturedAuth = request.headers[HttpHeaders.Authorization]
            respond(
                content = "[]",
                headers = headersOf(HttpHeaders.ContentType, "application/json"),
            )
        }

        val admin = AdminClient(
            "http://localhost:9094",
            authToken = "legacy-token",
            auth = AuthConfig.PlainAuth("admin", "secret"),
            httpClient = http,
        )
        admin.listTopics()

        // AuthConfig should take precedence over legacy authToken
        assertTrue(capturedAuth!!.startsWith("Basic "))
        admin.close()
    }

    @Test
    fun `AdminClient with no auth sends no header`() = runTest {
        var capturedAuth: String? = "should-be-null"
        val http = mockClient { request ->
            capturedAuth = request.headers[HttpHeaders.Authorization]
            respond(
                content = "[]",
                headers = headersOf(HttpHeaders.ContentType, "application/json"),
            )
        }

        val admin = AdminClient("http://localhost:9094", httpClient = http)
        admin.listTopics()

        assertEquals(null, capturedAuth)
        admin.close()
    }

    // -- authHeaders utility --

    @Test
    fun `authHeaders returns empty map for null`() = runTest {
        val headers = authHeaders(null)
        assertTrue(headers.isEmpty())
    }

    @Test
    fun `authHeaders returns Basic for PlainAuth`() = runTest {
        val headers = authHeaders(AuthConfig.PlainAuth("user", "pass"))
        assertEquals(1, headers.size)
        assertTrue(headers["Authorization"]!!.startsWith("Basic "))
    }

    @Test
    fun `authHeaders returns SCRAM for ScramAuth`() = runTest {
        val headers = authHeaders(AuthConfig.ScramAuth("user", "pass", ScramMechanism.SCRAM_SHA_256))
        assertEquals(1, headers.size)
        assertTrue(headers["Authorization"]!!.startsWith("SCRAM SCRAM_SHA_256 "))
    }

    @Test
    fun `authHeaders returns Bearer for OAuthBearerAuth`() = runTest {
        val headers = authHeaders(AuthConfig.OAuthBearerAuth {
            OAuthToken("token123", Long.MAX_VALUE)
        })
        assertEquals(1, headers.size)
        assertEquals("Bearer token123", headers["Authorization"])
    }

    // -- StreamlineClient auth parameter --

    @Test
    fun `StreamlineClient accepts auth parameter`() {
        val config = StreamlineConfiguration(url = "ws://localhost:9092")
        val client = StreamlineClient(config, auth = AuthConfig.PlainAuth("user", "pass"))
        assertEquals(ConnectionState.DISCONNECTED, client.state.value)
        client.close()
    }

    @Test
    fun `StreamlineClient accepts null auth`() {
        val config = StreamlineConfiguration(url = "ws://localhost:9092")
        val client = StreamlineClient(config, auth = null)
        assertEquals(ConnectionState.DISCONNECTED, client.state.value)
        client.close()
    }
}
