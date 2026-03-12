package io.streamline.sdk

import io.ktor.client.*
import io.ktor.client.engine.mock.*
import io.ktor.client.plugins.contentnegotiation.*
import io.ktor.client.request.*
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

    // -- Cluster Info --

    @Test
    fun `clusterInfo returns cluster details`() = runTest {
        val http = mockClient { request ->
            assertEquals("/v1/cluster", request.url.encodedPath)
            assertEquals(HttpMethod.Get, request.method)
            respond(
                content = """{
                    "cluster_id":"cluster-abc",
                    "broker_id":1,
                    "brokers":[
                        {"id":1,"host":"broker-1","port":9092,"rack":"us-east-1a"},
                        {"id":2,"host":"broker-2","port":9092}
                    ],
                    "controller":1
                }""",
                headers = headersOf(HttpHeaders.ContentType, "application/json"),
            )
        }

        val admin = AdminClient("http://localhost:9094", httpClient = http)
        val info = admin.clusterInfo()

        assertEquals("cluster-abc", info.clusterId)
        assertEquals(1, info.brokerId)
        assertEquals(2, info.brokers.size)
        assertEquals("broker-1", info.brokers[0].host)
        assertEquals("us-east-1a", info.brokers[0].rack)
        assertEquals(null, info.brokers[1].rack)
        assertEquals(1, info.controller)
        admin.close()
    }

    @Test
    fun `listBrokers delegates to clusterInfo`() = runTest {
        val http = mockClient {
            respond(
                content = """{"cluster_id":"c1","broker_id":0,"brokers":[{"id":1,"host":"h1","port":9092}],"controller":1}""",
                headers = headersOf(HttpHeaders.ContentType, "application/json"),
            )
        }

        val admin = AdminClient("http://localhost:9094", httpClient = http)
        val brokers = admin.listBrokers()
        assertEquals(1, brokers.size)
        assertEquals("h1", brokers[0].host)
        admin.close()
    }

    // -- Consumer Group Lag --

    @Test
    fun `consumerGroupLag returns lag details`() = runTest {
        val http = mockClient { request ->
            assertEquals("/v1/consumer-groups/my-group/lag", request.url.encodedPath)
            respond(
                content = """{
                    "group_id":"my-group",
                    "partitions":[
                        {"topic":"events","partition":0,"current_offset":50,"end_offset":100,"lag":50},
                        {"topic":"events","partition":1,"current_offset":80,"end_offset":100,"lag":20}
                    ],
                    "total_lag":70
                }""",
                headers = headersOf(HttpHeaders.ContentType, "application/json"),
            )
        }

        val admin = AdminClient("http://localhost:9094", httpClient = http)
        val lag = admin.consumerGroupLag("my-group")

        assertEquals("my-group", lag.groupId)
        assertEquals(2, lag.partitions.size)
        assertEquals(50L, lag.partitions[0].lag)
        assertEquals(20L, lag.partitions[1].lag)
        assertEquals(70L, lag.totalLag)
        admin.close()
    }

    @Test
    fun `consumerGroupTopicLag returns topic-scoped lag`() = runTest {
        val http = mockClient { request ->
            assertEquals("/v1/consumer-groups/my-group/lag/events", request.url.encodedPath)
            respond(
                content = """{
                    "group_id":"my-group",
                    "partitions":[{"topic":"events","partition":0,"current_offset":90,"end_offset":100,"lag":10}],
                    "total_lag":10
                }""",
                headers = headersOf(HttpHeaders.ContentType, "application/json"),
            )
        }

        val admin = AdminClient("http://localhost:9094", httpClient = http)
        val lag = admin.consumerGroupTopicLag("my-group", "events")

        assertEquals(1, lag.partitions.size)
        assertEquals("events", lag.partitions[0].topic)
        assertEquals(10L, lag.totalLag)
        admin.close()
    }

    @Test
    fun `consumerGroupLag with unknown group returns 404`() = runTest {
        val http = mockClient {
            respond(content = "", status = HttpStatusCode.NotFound)
        }

        val admin = AdminClient("http://localhost:9094", httpClient = http)
        assertFailsWith<TopicNotFoundException> {
            admin.consumerGroupLag("nonexistent")
        }
        admin.close()
    }

    // -- Reset Offsets --

    @Test
    fun `resetOffsetsDryRun returns projected changes`() = runTest {
        val http = mockClient { request ->
            assertTrue(request.url.encodedPath.endsWith("/reset-offsets/dry-run"))
            assertEquals(HttpMethod.Post, request.method)
            respond(
                content = """[
                    {"topic":"events","partition":0,"current_offset":100,"end_offset":100,"lag":0},
                    {"topic":"events","partition":1,"current_offset":50,"end_offset":100,"lag":50}
                ]""",
                headers = headersOf(HttpHeaders.ContentType, "application/json"),
            )
        }

        val admin = AdminClient("http://localhost:9094", httpClient = http)
        val result = admin.resetOffsetsDryRun("my-group", "events", "earliest")

        assertEquals(2, result.size)
        assertEquals(0L, result[0].lag)
        assertEquals(50L, result[1].lag)
        admin.close()
    }

    @Test
    fun `resetOffsets sends POST request`() = runTest {
        val http = mockClient { request ->
            assertTrue(request.url.encodedPath.endsWith("/reset-offsets"))
            assertFalse(request.url.encodedPath.contains("dry-run"))
            assertEquals(HttpMethod.Post, request.method)
            respond(content = "{}", headers = headersOf(HttpHeaders.ContentType, "application/json"))
        }

        val admin = AdminClient("http://localhost:9094", httpClient = http)
        admin.resetOffsets("my-group", "events", "latest")
        admin.close()
    }

    // -- Message Inspection --

    @Test
    fun `inspectMessages returns messages with headers`() = runTest {
        val http = mockClient { request ->
            assertTrue(request.url.encodedPath.startsWith("/v1/inspect/events"))
            assertTrue(request.url.encodedQuery?.contains("partition=0") == true)
            assertTrue(request.url.encodedQuery?.contains("limit=5") == true)
            respond(
                content = """[
                    {"offset":0,"key":"k1","value":"v1","timestamp":1000,"partition":0,"headers":{"source":"test"}},
                    {"offset":1,"value":"v2","timestamp":1001,"partition":0,"headers":{}}
                ]""",
                headers = headersOf(HttpHeaders.ContentType, "application/json"),
            )
        }

        val admin = AdminClient("http://localhost:9094", httpClient = http)
        val messages = admin.inspectMessages("events", partition = 0, limit = 5)

        assertEquals(2, messages.size)
        assertEquals("k1", messages[0].key)
        assertEquals("v1", messages[0].value)
        assertEquals(mapOf("source" to "test"), messages[0].headers)
        assertEquals(null, messages[1].key)
        admin.close()
    }

    @Test
    fun `inspectMessages with offset parameter`() = runTest {
        val http = mockClient { request ->
            assertTrue(request.url.encodedQuery?.contains("offset=100") == true)
            respond(content = "[]", headers = headersOf(HttpHeaders.ContentType, "application/json"))
        }

        val admin = AdminClient("http://localhost:9094", httpClient = http)
        val messages = admin.inspectMessages("events", offset = 100)
        assertEquals(0, messages.size)
        admin.close()
    }

    @Test
    fun `latestMessages returns recent messages`() = runTest {
        val http = mockClient { request ->
            assertTrue(request.url.encodedPath.endsWith("/latest"))
            assertTrue(request.url.encodedQuery?.contains("count=3") == true)
            respond(
                content = """[
                    {"offset":97,"value":"msg1","timestamp":3000,"partition":0,"headers":{}},
                    {"offset":98,"value":"msg2","timestamp":3001,"partition":0,"headers":{}},
                    {"offset":99,"value":"msg3","timestamp":3002,"partition":0,"headers":{}}
                ]""",
                headers = headersOf(HttpHeaders.ContentType, "application/json"),
            )
        }

        val admin = AdminClient("http://localhost:9094", httpClient = http)
        val messages = admin.latestMessages("events", count = 3)

        assertEquals(3, messages.size)
        assertEquals(97L, messages[0].offset)
        assertEquals("msg3", messages[2].value)
        admin.close()
    }

    // -- Metrics --

    @Test
    fun `metricsHistory returns metric points`() = runTest {
        val http = mockClient { request ->
            assertEquals("/v1/metrics/history", request.url.encodedPath)
            respond(
                content = """[
                    {"name":"bytes_in","value":"1024.5","labels":{"topic":"events"},"timestamp":1000},
                    {"name":"bytes_out","value":"512.0","labels":{},"timestamp":1001}
                ]""",
                headers = headersOf(HttpHeaders.ContentType, "application/json"),
            )
        }

        val admin = AdminClient("http://localhost:9094", httpClient = http)
        val metrics = admin.metricsHistory()

        assertEquals(2, metrics.size)
        assertEquals("bytes_in", metrics[0].name)
        assertEquals(1024.5, metrics[0].value)
        assertEquals(mapOf("topic" to "events"), metrics[0].labels)
        assertEquals(512.0, metrics[1].value)
        admin.close()
    }

    @Test
    fun `metricsHistory empty response`() = runTest {
        val http = mockClient {
            respond(content = "[]", headers = headersOf(HttpHeaders.ContentType, "application/json"))
        }

        val admin = AdminClient("http://localhost:9094", httpClient = http)
        val metrics = admin.metricsHistory()
        assertEquals(0, metrics.size)
        admin.close()
    }

    // -- Error cases for new methods --

    @Test
    fun `clusterInfo unauthorized throws AuthenticationFailedException`() = runTest {
        val http = mockClient {
            respond(content = "Unauthorized", status = HttpStatusCode.Unauthorized)
        }

        val admin = AdminClient("http://localhost:9094", httpClient = http)
        assertFailsWith<AuthenticationFailedException> {
            admin.clusterInfo()
        }
        admin.close()
    }

    @Test
    fun `inspectMessages server error throws AdminOperationException`() = runTest {
        val http = mockClient {
            respond(
                content = """{"error":"internal"}""",
                status = HttpStatusCode.InternalServerError,
                headers = headersOf(HttpHeaders.ContentType, "application/json"),
            )
        }

        val admin = AdminClient("http://localhost:9094", httpClient = http)
        assertFailsWith<AdminOperationException> {
            admin.inspectMessages("events")
        }
        admin.close()
    }

    // -- ACL Operations --

    @Test
    fun `listAcls returns parsed entries`() = runTest {
        val http = mockClient { request ->
            assertEquals("/v1/acls", request.url.encodedPath)
            assertEquals(HttpMethod.Get, request.method)
            respond(
                content = """[
                    {"principal":"User:alice","resource_type":"topic","resource_name":"events","operation":"read","permission":"allow","host":"*"},
                    {"principal":"User:bob","resource_type":"group","resource_name":"my-group","operation":"read","permission":"allow","host":"*"}
                ]""",
                headers = headersOf(HttpHeaders.ContentType, "application/json"),
            )
        }

        val admin = AdminClient("http://localhost:9094", httpClient = http)
        val acls = admin.listAcls()

        assertEquals(2, acls.size)
        assertEquals("User:alice", acls[0].principal)
        assertEquals("topic", acls[0].resourceType)
        assertEquals("events", acls[0].resourceName)
        assertEquals("read", acls[0].operation)
        assertEquals("allow", acls[0].permission)
        assertEquals("User:bob", acls[1].principal)
        admin.close()
    }

    @Test
    fun `listAcls with resource type filter`() = runTest {
        val http = mockClient { request ->
            assertTrue(request.url.encodedPath.contains("/v1/acls"))
            assertEquals("topic", request.url.parameters["resourceType"])
            respond(
                content = """[{"principal":"User:alice","resource_type":"topic","resource_name":"events","operation":"read","permission":"allow","host":"*"}]""",
                headers = headersOf(HttpHeaders.ContentType, "application/json"),
            )
        }

        val admin = AdminClient("http://localhost:9094", httpClient = http)
        val acls = admin.listAcls(AclResourceType.TOPIC)
        assertEquals(1, acls.size)
        admin.close()
    }

    @Test
    fun `listAcls empty returns empty list`() = runTest {
        val http = mockClient {
            respond(content = "[]", headers = headersOf(HttpHeaders.ContentType, "application/json"))
        }

        val admin = AdminClient("http://localhost:9094", httpClient = http)
        val acls = admin.listAcls()
        assertTrue(acls.isEmpty())
        admin.close()
    }

    @Test
    fun `createAcl sends correct request`() = runTest {
        val http = mockClient { request ->
            assertEquals("/v1/acls", request.url.encodedPath)
            assertEquals(HttpMethod.Post, request.method)
            val body = request.body.toByteArray().decodeToString()
            assertTrue(body.contains("\"principal\":\"User:alice\""))
            assertTrue(body.contains("\"resource_type\":\"topic\""))
            assertTrue(body.contains("\"resource_name\":\"events\""))
            assertTrue(body.contains("\"operation\":\"read\""))
            assertTrue(body.contains("\"permission\":\"allow\""))
            respond(
                content = """{"created":true}""",
                headers = headersOf(HttpHeaders.ContentType, "application/json"),
            )
        }

        val admin = AdminClient("http://localhost:9094", httpClient = http)
        admin.createAcl(
            principal = "User:alice",
            resourceType = AclResourceType.TOPIC,
            resourceName = "events",
            operation = AclOperation.READ,
            permission = AclPermission.ALLOW,
        )
        admin.close()
    }

    @Test
    fun `deleteAcl sends correct filter parameters`() = runTest {
        val http = mockClient { request ->
            assertTrue(request.url.encodedPath.startsWith("/v1/acls"))
            assertEquals(HttpMethod.Delete, request.method)
            assertEquals("User:alice", request.url.parameters["principal"])
            assertEquals("topic", request.url.parameters["resource_type"])
            assertEquals("events", request.url.parameters["resource_name"])
            assertEquals("write", request.url.parameters["operation"])
            respond(
                content = """{"deleted":1}""",
                headers = headersOf(HttpHeaders.ContentType, "application/json"),
            )
        }

        val admin = AdminClient("http://localhost:9094", httpClient = http)
        admin.deleteAcl(
            principal = "User:alice",
            resourceType = AclResourceType.TOPIC,
            resourceName = "events",
            operation = AclOperation.WRITE,
        )
        admin.close()
    }

    @Test
    fun `createAcl unauthorized throws AuthenticationFailedException`() = runTest {
        val http = mockClient {
            respond(
                content = """{"error":"unauthorized"}""",
                status = HttpStatusCode.Unauthorized,
                headers = headersOf(HttpHeaders.ContentType, "application/json"),
            )
        }

        val admin = AdminClient("http://localhost:9094", httpClient = http)
        assertFailsWith<AuthenticationFailedException> {
            admin.createAcl(
                principal = "User:alice",
                resourceType = AclResourceType.TOPIC,
                resourceName = "events",
                operation = AclOperation.READ,
            )
        }
        admin.close()
    }
}
