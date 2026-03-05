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

class AdminClientTest {

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

    // -- List Topics --

    @Test
    fun `listTopics returns parsed topics`() = runTest {
        val http = mockClient { request ->
            assertEquals("/v1/topics", request.url.encodedPath)
            assertEquals(HttpMethod.Get, request.method)
            respond(
                content = """[
                    {"name":"events","partitions":3,"replication_factor":1,"message_count":100},
                    {"name":"logs","partitions":1,"replication_factor":1,"message_count":50}
                ]""",
                headers = headersOf(HttpHeaders.ContentType, "application/json"),
            )
        }

        val admin = AdminClient("http://localhost:9094", httpClient = http)
        val topics = admin.listTopics()

        assertEquals(2, topics.size)
        assertEquals("events", topics[0].name)
        assertEquals(3, topics[0].partitions)
        assertEquals(100L, topics[0].messageCount)
        assertEquals("logs", topics[1].name)
        admin.close()
    }

    @Test
    fun `listTopics empty list`() = runTest {
        val http = mockClient {
            respond(content = "[]", headers = headersOf(HttpHeaders.ContentType, "application/json"))
        }

        val admin = AdminClient("http://localhost:9094", httpClient = http)
        val topics = admin.listTopics()
        assertTrue(topics.isEmpty())
        admin.close()
    }

    // -- Describe Topic --

    @Test
    fun `describeTopic returns full info`() = runTest {
        val http = mockClient {
            respond(
                content = """{"name":"events","partitions":6,"replication_factor":3,"message_count":5000,"config":{"retention.ms":"86400000"}}""",
                headers = headersOf(HttpHeaders.ContentType, "application/json"),
            )
        }

        val admin = AdminClient("http://localhost:9094", httpClient = http)
        val desc = admin.describeTopic("events")

        assertEquals("events", desc.name)
        assertEquals(6, desc.partitions)
        assertEquals(3, desc.replicationFactor)
        assertEquals(5000L, desc.messageCount)
        assertEquals("86400000", desc.config["retention.ms"])
        admin.close()
    }

    @Test
    fun `describeTopic not found throws TopicNotFoundException`() = runTest {
        val http = mockClient {
            respond(
                content = """{"error":"topic not found"}""",
                status = HttpStatusCode.NotFound,
                headers = headersOf(HttpHeaders.ContentType, "application/json"),
            )
        }

        val admin = AdminClient("http://localhost:9094", httpClient = http)
        assertFailsWith<TopicNotFoundException> {
            admin.describeTopic("nonexistent")
        }
        admin.close()
    }

    // -- Create Topic --

    @Test
    fun `createTopic sends correct request`() = runTest {
        var capturedBody = ""
        val http = mockClient { request ->
            assertEquals(HttpMethod.Post, request.method)
            assertEquals("/v1/topics", request.url.encodedPath)
            capturedBody = String(request.body.toByteArray())
            respond(
                content = """{"status":"created"}""",
                status = HttpStatusCode.Created,
                headers = headersOf(HttpHeaders.ContentType, "application/json"),
            )
        }

        val admin = AdminClient("http://localhost:9094", httpClient = http)
        admin.createTopic("new-topic", partitions = 3, replicationFactor = 1)

        assertTrue(capturedBody.contains("\"name\":\"new-topic\""))
        assertTrue(capturedBody.contains("\"partitions\":3"))
        admin.close()
    }

    @Test
    fun `createTopic with config`() = runTest {
        var capturedBody = ""
        val http = mockClient { request ->
            capturedBody = String(request.body.toByteArray())
            respond(content = "{}", headers = headersOf(HttpHeaders.ContentType, "application/json"))
        }

        val admin = AdminClient("http://localhost:9094", httpClient = http)
        admin.createTopic("t", config = mapOf("retention.ms" to "3600000"))

        assertTrue(capturedBody.contains("retention.ms"))
        assertTrue(capturedBody.contains("3600000"))
        admin.close()
    }

    // -- Delete Topic --

    @Test
    fun `deleteTopic sends DELETE request`() = runTest {
        val http = mockClient { request ->
            assertEquals(HttpMethod.Delete, request.method)
            assertTrue(request.url.encodedPath.endsWith("/v1/topics/old-topic"))
            respond(content = "", status = HttpStatusCode.NoContent)
        }

        val admin = AdminClient("http://localhost:9094", httpClient = http)
        admin.deleteTopic("old-topic")
        admin.close()
    }

    // -- Consumer Groups --

    @Test
    fun `listConsumerGroups returns groups`() = runTest {
        val http = mockClient {
            respond(
                content = """[
                    {"id":"group-1","members":["m1","m2"],"state":"Stable"},
                    {"id":"group-2","members":[],"state":"Empty"}
                ]""",
                headers = headersOf(HttpHeaders.ContentType, "application/json"),
            )
        }

        val admin = AdminClient("http://localhost:9094", httpClient = http)
        val groups = admin.listConsumerGroups()

        assertEquals(2, groups.size)
        assertEquals("group-1", groups[0].id)
        assertEquals(2, groups[0].members.size)
        assertEquals("Stable", groups[0].state)
        assertEquals("Empty", groups[1].state)
        admin.close()
    }

    @Test
    fun `describeConsumerGroup returns details`() = runTest {
        val http = mockClient {
            respond(
                content = """{
                    "id":"cg-1",
                    "state":"Stable",
                    "members":[{"id":"m1","client_id":"client-1","host":"10.0.0.1","assignments":["events-0"]}],
                    "protocol":"range"
                }""",
                headers = headersOf(HttpHeaders.ContentType, "application/json"),
            )
        }

        val admin = AdminClient("http://localhost:9094", httpClient = http)
        val desc = admin.describeConsumerGroup("cg-1")

        assertEquals("cg-1", desc.id)
        assertEquals("Stable", desc.state)
        assertEquals(1, desc.members.size)
        assertEquals("client-1", desc.members[0].clientId)
        assertEquals(listOf("events-0"), desc.members[0].assignments)
        assertEquals("range", desc.protocol)
        admin.close()
    }

    @Test
    fun `deleteConsumerGroup sends DELETE`() = runTest {
        val http = mockClient { request ->
            assertEquals(HttpMethod.Delete, request.method)
            respond(content = "", status = HttpStatusCode.NoContent)
        }

        val admin = AdminClient("http://localhost:9094", httpClient = http)
        admin.deleteConsumerGroup("old-group")
        admin.close()
    }

    // -- Query --

    @Test
    fun `query returns results`() = runTest {
        val http = mockClient { request ->
            assertEquals(HttpMethod.Post, request.method)
            assertEquals("/v1/query", request.url.encodedPath)
            respond(
                content = """{
                    "columns":["key","value","offset"],
                    "rows":[["k1","v1","0"],["k2","v2","1"]],
                    "row_count":2
                }""",
                headers = headersOf(HttpHeaders.ContentType, "application/json"),
            )
        }

        val admin = AdminClient("http://localhost:9094", httpClient = http)
        val result = admin.query("SELECT * FROM events LIMIT 10")

        assertEquals(listOf("key", "value", "offset"), result.columns)
        assertEquals(2, result.rows.size)
        assertEquals("k1", result.rows[0][0])
        assertEquals(2, result.rowCount)
        admin.close()
    }

    @Test
    fun `query empty result`() = runTest {
        val http = mockClient {
            respond(
                content = """{"columns":["key"],"rows":[],"row_count":0}""",
                headers = headersOf(HttpHeaders.ContentType, "application/json"),
            )
        }

        val admin = AdminClient("http://localhost:9094", httpClient = http)
        val result = admin.query("SELECT * FROM empty")
        assertEquals(0, result.rowCount)
        assertTrue(result.rows.isEmpty())
        admin.close()
    }

    // -- Server Info --

    @Test
    fun `serverInfo returns info`() = runTest {
        val http = mockClient {
            respond(
                content = """{"version":"0.2.0","uptime":3600,"topic_count":5,"message_count":10000}""",
                headers = headersOf(HttpHeaders.ContentType, "application/json"),
            )
        }

        val admin = AdminClient("http://localhost:9094", httpClient = http)
        val info = admin.serverInfo()

        assertEquals("0.2.0", info.version)
        assertEquals(3600L, info.uptime)
        assertEquals(5, info.topicCount)
        assertEquals(10000L, info.messageCount)
        admin.close()
    }

    // -- Health Check --

    @Test
    fun `isHealthy returns true on success`() = runTest {
        val http = mockClient {
            respond(content = "OK", status = HttpStatusCode.OK)
        }

        val admin = AdminClient("http://localhost:9094", httpClient = http)
        assertTrue(admin.isHealthy())
        admin.close()
    }

    @Test
    fun `isHealthy returns false on error`() = runTest {
        val http = mockClient {
            respond(content = "", status = HttpStatusCode.ServiceUnavailable)
        }

        val admin = AdminClient("http://localhost:9094", httpClient = http)
        assertFalse(admin.isHealthy())
        admin.close()
    }

    // -- Auth Header --

    @Test
    fun `auth token is sent in header`() = runTest {
        val http = mockClient { request ->
            assertEquals("Bearer my-token", request.headers[HttpHeaders.Authorization])
            respond(content = "[]", headers = headersOf(HttpHeaders.ContentType, "application/json"))
        }

        val admin = AdminClient("http://localhost:9094", authToken = "my-token", httpClient = http)
        admin.listTopics()
        admin.close()
    }

    @Test
    fun `no auth header when token is null`() = runTest {
        val http = mockClient { request ->
            assertEquals(null, request.headers[HttpHeaders.Authorization])
            respond(content = "[]", headers = headersOf(HttpHeaders.ContentType, "application/json"))
        }

        val admin = AdminClient("http://localhost:9094", httpClient = http)
        admin.listTopics()
        admin.close()
    }

    // -- Error Handling --

    @Test
    fun `unauthorized throws AuthenticationFailedException`() = runTest {
        val http = mockClient {
            respond(
                content = """{"error":"unauthorized"}""",
                status = HttpStatusCode.Unauthorized,
                headers = headersOf(HttpHeaders.ContentType, "application/json"),
            )
        }

        val admin = AdminClient("http://localhost:9094", httpClient = http)
        assertFailsWith<AuthenticationFailedException> {
            admin.listTopics()
        }
        admin.close()
    }

    @Test
    fun `server error throws AdminOperationException`() = runTest {
        val http = mockClient {
            respond(
                content = """{"error":"internal error"}""",
                status = HttpStatusCode.InternalServerError,
                headers = headersOf(HttpHeaders.ContentType, "application/json"),
            )
        }

        val admin = AdminClient("http://localhost:9094", httpClient = http)
        assertFailsWith<AdminOperationException> {
            admin.listTopics()
        }
        admin.close()
    }
}
