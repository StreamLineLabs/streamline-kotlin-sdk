package io.streamline.sdk

import io.ktor.client.*
import io.ktor.client.engine.mock.*
import io.ktor.client.plugins.contentnegotiation.*
import io.ktor.http.*
import io.ktor.serialization.kotlinx.json.*
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.json.Json
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class SchemaRegistryClientTest {

    private fun mockClient(handler: MockRequestHandleScope.(HttpRequestData) -> HttpResponseData): HttpClient {
        return HttpClient(MockEngine) {
            engine {
                addHandler { request -> handler(request) }
            }
            install(ContentNegotiation) {
                json(Json { ignoreUnknownKeys = true; encodeDefaults = true })
            }
        }
    }

    // -- Schema Registration --

    @Test
    fun `registerSchema returns schema id`() = runTest {
        var capturedBody = ""
        val http = mockClient { request ->
            assertEquals(HttpMethod.Post, request.method)
            assertTrue(request.url.encodedPath.endsWith("/subjects/events-value/versions"))
            capturedBody = String(request.body.toByteArray())
            respond(
                content = """{"id":42}""",
                headers = headersOf(HttpHeaders.ContentType, "application/json"),
            )
        }

        val registry = SchemaRegistryClient("http://localhost:9094", httpClient = http)
        val id = registry.registerSchema("events-value", """{"type":"record","name":"Event","fields":[]}""", SchemaFormat.AVRO)

        assertEquals(42, id)
        assertTrue(capturedBody.contains("\"schemaType\":\"AVRO\""))
        assertTrue(capturedBody.contains("\"schema\""))
        registry.close()
    }

    @Test
    fun `registerSchema with PROTOBUF format`() = runTest {
        var capturedBody = ""
        val http = mockClient { request ->
            capturedBody = String(request.body.toByteArray())
            respond(
                content = """{"id":7}""",
                headers = headersOf(HttpHeaders.ContentType, "application/json"),
            )
        }

        val registry = SchemaRegistryClient("http://localhost:9094", httpClient = http)
        val id = registry.registerSchema("user-value", "syntax = \"proto3\";", SchemaFormat.PROTOBUF)

        assertEquals(7, id)
        assertTrue(capturedBody.contains("\"schemaType\":\"PROTOBUF\""))
        registry.close()
    }

    @Test
    fun `registerSchema with JSON format`() = runTest {
        var capturedBody = ""
        val http = mockClient { request ->
            capturedBody = String(request.body.toByteArray())
            respond(
                content = """{"id":15}""",
                headers = headersOf(HttpHeaders.ContentType, "application/json"),
            )
        }

        val registry = SchemaRegistryClient("http://localhost:9094", httpClient = http)
        val id = registry.registerSchema("order-value", """{"type":"object"}""", SchemaFormat.JSON)

        assertEquals(15, id)
        assertTrue(capturedBody.contains("\"schemaType\":\"JSON\""))
        registry.close()
    }

    // -- Schema Retrieval --

    @Test
    fun `getLatestSchema returns schema info`() = runTest {
        val http = mockClient { request ->
            assertEquals(HttpMethod.Get, request.method)
            assertTrue(request.url.encodedPath.endsWith("/subjects/events-value/versions/latest"))
            respond(
                content = """{
                    "subject":"events-value",
                    "id":42,
                    "version":3,
                    "schemaType":"AVRO",
                    "schema":"{\"type\":\"record\",\"name\":\"Event\",\"fields\":[]}"
                }""",
                headers = headersOf(HttpHeaders.ContentType, "application/json"),
            )
        }

        val registry = SchemaRegistryClient("http://localhost:9094", httpClient = http)
        val info = registry.getLatestSchema("events-value")

        assertEquals("events-value", info.subject)
        assertEquals(42, info.id)
        assertEquals(3, info.version)
        assertEquals("AVRO", info.schemaType)
        assertTrue(info.schema.contains("Event"))
        registry.close()
    }

    @Test
    fun `getSchemaVersion returns specific version`() = runTest {
        val http = mockClient { request ->
            assertEquals(HttpMethod.Get, request.method)
            assertTrue(request.url.encodedPath.endsWith("/subjects/events-value/versions/2"))
            respond(
                content = """{
                    "subject":"events-value",
                    "id":10,
                    "version":2,
                    "schemaType":"AVRO",
                    "schema":"{\"type\":\"record\"}"
                }""",
                headers = headersOf(HttpHeaders.ContentType, "application/json"),
            )
        }

        val registry = SchemaRegistryClient("http://localhost:9094", httpClient = http)
        val info = registry.getSchemaVersion("events-value", 2)

        assertEquals("events-value", info.subject)
        assertEquals(10, info.id)
        assertEquals(2, info.version)
        registry.close()
    }

    @Test
    fun `getSchemaById returns schema for global id`() = runTest {
        val http = mockClient { request ->
            assertEquals(HttpMethod.Get, request.method)
            assertTrue(request.url.encodedPath.endsWith("/schemas/ids/42"))
            respond(
                content = """{
                    "subject":"events-value",
                    "id":42,
                    "version":1,
                    "schemaType":"AVRO",
                    "schema":"{\"type\":\"string\"}"
                }""",
                headers = headersOf(HttpHeaders.ContentType, "application/json"),
            )
        }

        val registry = SchemaRegistryClient("http://localhost:9094", httpClient = http)
        val info = registry.getSchemaById(42)

        assertEquals(42, info.id)
        assertEquals("events-value", info.subject)
        registry.close()
    }

    // -- Subject Management --

    @Test
    fun `listSubjects returns subject names`() = runTest {
        val http = mockClient { request ->
            assertEquals(HttpMethod.Get, request.method)
            assertTrue(request.url.encodedPath.endsWith("/subjects"))
            respond(
                content = """["events-value","users-value","orders-key"]""",
                headers = headersOf(HttpHeaders.ContentType, "application/json"),
            )
        }

        val registry = SchemaRegistryClient("http://localhost:9094", httpClient = http)
        val subjects = registry.listSubjects()

        assertEquals(3, subjects.size)
        assertEquals("events-value", subjects[0])
        assertEquals("users-value", subjects[1])
        assertEquals("orders-key", subjects[2])
        registry.close()
    }

    @Test
    fun `listSubjects empty`() = runTest {
        val http = mockClient {
            respond(
                content = "[]",
                headers = headersOf(HttpHeaders.ContentType, "application/json"),
            )
        }

        val registry = SchemaRegistryClient("http://localhost:9094", httpClient = http)
        val subjects = registry.listSubjects()
        assertTrue(subjects.isEmpty())
        registry.close()
    }

    @Test
    fun `listVersions returns version numbers`() = runTest {
        val http = mockClient { request ->
            assertEquals(HttpMethod.Get, request.method)
            assertTrue(request.url.encodedPath.endsWith("/subjects/events-value/versions"))
            respond(
                content = "[1,2,3,4]",
                headers = headersOf(HttpHeaders.ContentType, "application/json"),
            )
        }

        val registry = SchemaRegistryClient("http://localhost:9094", httpClient = http)
        val versions = registry.listVersions("events-value")

        assertEquals(listOf(1, 2, 3, 4), versions)
        registry.close()
    }

    @Test
    fun `deleteSubject returns deleted versions`() = runTest {
        val http = mockClient { request ->
            assertEquals(HttpMethod.Delete, request.method)
            assertTrue(request.url.encodedPath.endsWith("/subjects/events-value"))
            respond(
                content = "[1,2,3]",
                headers = headersOf(HttpHeaders.ContentType, "application/json"),
            )
        }

        val registry = SchemaRegistryClient("http://localhost:9094", httpClient = http)
        val deleted = registry.deleteSubject("events-value")

        assertEquals(listOf(1, 2, 3), deleted)
        registry.close()
    }

    // -- Compatibility --

    @Test
    fun `checkCompatibility returns true when compatible`() = runTest {
        var capturedBody = ""
        val http = mockClient { request ->
            assertEquals(HttpMethod.Post, request.method)
            assertTrue(request.url.encodedPath.endsWith("/compatibility/subjects/events-value/versions/latest"))
            capturedBody = String(request.body.toByteArray())
            respond(
                content = """{"is_compatible":"true"}""",
                headers = headersOf(HttpHeaders.ContentType, "application/json"),
            )
        }

        val registry = SchemaRegistryClient("http://localhost:9094", httpClient = http)
        val compatible = registry.checkCompatibility("events-value", """{"type":"record"}""")

        assertTrue(compatible)
        assertTrue(capturedBody.contains("\"schema\""))
        registry.close()
    }

    @Test
    fun `checkCompatibility returns false when incompatible`() = runTest {
        val http = mockClient {
            respond(
                content = """{"is_compatible":"false"}""",
                headers = headersOf(HttpHeaders.ContentType, "application/json"),
            )
        }

        val registry = SchemaRegistryClient("http://localhost:9094", httpClient = http)
        val compatible = registry.checkCompatibility("events-value", """{"type":"string"}""")

        assertFalse(compatible)
        registry.close()
    }

    // -- Error Handling --

    @Test
    fun `not found throws SchemaRegistryException`() = runTest {
        val http = mockClient {
            respond(
                content = """{"error_code":40401,"message":"Subject not found"}""",
                status = HttpStatusCode.NotFound,
                headers = headersOf(HttpHeaders.ContentType, "application/json"),
            )
        }

        val registry = SchemaRegistryClient("http://localhost:9094", httpClient = http)
        val ex = assertFailsWith<SchemaRegistryException> {
            registry.getLatestSchema("nonexistent")
        }
        assertTrue(ex.message!!.contains("Not found"))
        registry.close()
    }

    @Test
    fun `unauthorized throws SchemaRegistryException`() = runTest {
        val http = mockClient {
            respond(
                content = """{"error":"unauthorized"}""",
                status = HttpStatusCode.Unauthorized,
                headers = headersOf(HttpHeaders.ContentType, "application/json"),
            )
        }

        val registry = SchemaRegistryClient("http://localhost:9094", httpClient = http)
        val ex = assertFailsWith<SchemaRegistryException> {
            registry.listSubjects()
        }
        assertTrue(ex.message!!.contains("Unauthorized"))
        registry.close()
    }

    @Test
    fun `server error throws SchemaRegistryException`() = runTest {
        val http = mockClient {
            respond(
                content = """{"error":"internal error"}""",
                status = HttpStatusCode.InternalServerError,
                headers = headersOf(HttpHeaders.ContentType, "application/json"),
            )
        }

        val registry = SchemaRegistryClient("http://localhost:9094", httpClient = http)
        assertFailsWith<SchemaRegistryException> {
            registry.registerSchema("x", "{}")
        }
        registry.close()
    }

    // -- Auth Token --

    @Test
    fun `auth token is sent in header`() = runTest {
        val http = mockClient { request ->
            assertEquals("Bearer my-secret-token", request.headers[HttpHeaders.Authorization])
            respond(
                content = "[]",
                headers = headersOf(HttpHeaders.ContentType, "application/json"),
            )
        }

        val registry = SchemaRegistryClient("http://localhost:9094", authToken = "my-secret-token", httpClient = http)
        registry.listSubjects()
        registry.close()
    }

    @Test
    fun `no auth header when token is null`() = runTest {
        val http = mockClient { request ->
            assertEquals(null, request.headers[HttpHeaders.Authorization])
            respond(
                content = "[]",
                headers = headersOf(HttpHeaders.ContentType, "application/json"),
            )
        }

        val registry = SchemaRegistryClient("http://localhost:9094", httpClient = http)
        registry.listSubjects()
        registry.close()
    }

    // -- Client Construction --

    @Test
    fun `client with default parameters`() = runTest {
        val http = mockClient {
            respond(
                content = "[]",
                headers = headersOf(HttpHeaders.ContentType, "application/json"),
            )
        }

        val registry = SchemaRegistryClient("http://localhost:9094", httpClient = http)
        val subjects = registry.listSubjects()
        assertTrue(subjects.isEmpty())
        registry.close()
    }

    @Test
    fun `client with custom base url`() = runTest {
        val http = mockClient { request ->
            assertTrue(request.url.toString().startsWith("http://schema-registry:8081"))
            respond(
                content = "[]",
                headers = headersOf(HttpHeaders.ContentType, "application/json"),
            )
        }

        val registry = SchemaRegistryClient("http://schema-registry:8081", httpClient = http)
        registry.listSubjects()
        registry.close()
    }
}
