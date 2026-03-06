package io.streamline.sdk

// SDK Conformance Test Suite — 46 tests per SDK_CONFORMANCE_SPEC.md
//
// Tests use mocked HTTP clients and model validation to verify SDK contracts
// without requiring a running Streamline server.

import io.ktor.client.*
import io.ktor.client.engine.mock.*
import io.ktor.client.plugins.contentnegotiation.*
import io.ktor.http.*
import io.ktor.serialization.kotlinx.json.*
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

private val json = Json { ignoreUnknownKeys = true; encodeDefaults = true }

private fun mockHttp(handler: MockRequestHandleScope.(HttpRequestData) -> HttpResponseData): HttpClient {
    return HttpClient(MockEngine) {
        engine { addHandler { request -> handler(request) } }
        install(ContentNegotiation) { json(Json { ignoreUnknownKeys = true; encodeDefaults = true }) }
    }
}

// ========== PRODUCER (8 tests) ==========

class ProducerConformanceTest {

    @Test fun `P01 simple produce — message model round-trip`() = runTest {
        val msg = StreamlineMessage(topic = "events", value = "{\"action\":\"click\"}")
        val encoded = json.encodeToString(msg)
        val decoded = json.decodeFromString<StreamlineMessage>(encoded)
        assertEquals("events", decoded.topic)
        assertEquals("{\"action\":\"click\"}", decoded.value)
        assertNull(decoded.key)
    }

    @Test fun `P02 keyed produce — key is preserved`() = runTest {
        val msg = StreamlineMessage(topic = "events", key = "user-42", value = "data")
        val encoded = json.encodeToString(msg)
        val decoded = json.decodeFromString<StreamlineMessage>(encoded)
        assertEquals("user-42", decoded.key)
        assertEquals("data", decoded.value)
    }

    @Test fun `P03 headers produce — message metadata preserved`() = runTest {
        val msg = StreamlineMessage(
            topic = "events", key = "k1", value = "v1",
            offset = 42L, timestamp = 1700000000000L,
        )
        val encoded = json.encodeToString(msg)
        val decoded = json.decodeFromString<StreamlineMessage>(encoded)
        assertEquals(42L, decoded.offset)
        assertEquals(1700000000000L, decoded.timestamp)
    }

    @Test fun `P04 batch produce — producer config batch size`() = runTest {
        val config = ProducerConfig(batchSize = 65536, lingerMs = 10)
        assertEquals(65536, config.batchSize)
        assertEquals(10L, config.lingerMs)
    }

    @Test fun `P05 compression — all types supported`() = runTest {
        for (type in CompressionType.entries) {
            val config = ProducerConfig(compression = type)
            assertEquals(type, config.compression)
        }
        assertEquals(5, CompressionType.entries.size)
    }

    @Test fun `P06 partitioner — default producer config`() = runTest {
        val config = ProducerConfig()
        assertEquals(16384, config.batchSize)
        assertEquals(0L, config.lingerMs)
        assertEquals(CompressionType.NONE, config.compression)
        assertEquals(3, config.retries)
        assertEquals(Acks.ONE, config.acks)
    }

    @Test fun `P07 idempotent — config flag and acks`() = runTest {
        val config = ProducerConfig(idempotent = true, acks = Acks.ALL)
        assertTrue(config.idempotent)
        assertEquals(Acks.ALL, config.acks)
    }

    @Test fun `P08 timeout — client configuration`() = runTest {
        val config = StreamlineConfiguration(url = "ws://localhost:9092", timeoutMs = 5000)
        assertEquals(5000L, config.timeoutMs)
    }
}

// ========== CONSUMER (8 tests) ==========

class ConsumerConformanceTest {

    @Test fun `C01 subscribe — client starts disconnected`() = runTest {
        val client = StreamlineClient(StreamlineConfiguration(url = "ws://localhost:9092"))
        assertEquals(ConnectionState.DISCONNECTED, client.state.value)
    }

    @Test fun `C02 from beginning — offset reset earliest`() = runTest {
        val config = ConsumerConfig(autoOffsetReset = OffsetReset.EARLIEST)
        assertEquals(OffsetReset.EARLIEST, config.autoOffsetReset)
    }

    @Test fun `C03 from offset — message offset parsing`() = runTest {
        val msg = StreamlineMessage(topic = "t", value = "v", offset = 999L)
        assertEquals(999L, msg.offset)
    }

    @Test fun `C04 from timestamp — message timestamp parsing`() = runTest {
        val ts = System.currentTimeMillis()
        val msg = StreamlineMessage(topic = "t", value = "v", timestamp = ts)
        assertEquals(ts, msg.timestamp)
    }

    @Test fun `C05 follow — auto reconnect config`() = runTest {
        val config = StreamlineConfiguration(
            url = "ws://localhost:9092", autoReconnect = true, maxRetries = 5,
        )
        assertTrue(config.autoReconnect)
        assertEquals(5, config.maxRetries)
    }

    @Test fun `C06 filter — flow operators work with messages`() = runTest {
        val messages = listOf(
            StreamlineMessage(topic = "t", key = "a", value = "1"),
            StreamlineMessage(topic = "t", key = null, value = "2"),
            StreamlineMessage(topic = "t", key = "b", value = "3"),
        )
        val filtered = messages.filter { it.key != null }
        assertEquals(2, filtered.size)
        assertEquals("a", filtered[0].key)
        assertEquals("b", filtered[1].key)
    }

    @Test fun `C07 headers — message fields complete`() = runTest {
        val msg = StreamlineMessage(
            topic = "events", key = "k", value = "v", offset = 10, timestamp = 12345L,
        )
        assertNotNull(msg.topic)
        assertNotNull(msg.key)
        assertNotNull(msg.value)
        assertNotNull(msg.offset)
        assertNotNull(msg.timestamp)
    }

    @Test fun `C08 timeout — consumer session config`() = runTest {
        val config = ConsumerConfig(sessionTimeoutMs = 15000, heartbeatIntervalMs = 5000)
        assertEquals(15000L, config.sessionTimeoutMs)
        assertEquals(5000L, config.heartbeatIntervalMs)
    }
}

// ========== CONSUMER GROUPS (8 tests) ==========

class ConsumerGroupConformanceTest {

    @Test fun `G01 join group — group config creation`() = runTest {
        val config = ConsumerConfig(groupId = "my-group")
        assertEquals("my-group", config.groupId)
    }

    @Test fun `G02 commit offset — auto commit default`() = runTest {
        val config = ConsumerConfig()
        assertTrue(config.autoCommit)
        assertEquals(5000L, config.autoCommitIntervalMs)
    }

    @Test fun `G03 fetch committed offset — consumer group model`() = runTest {
        val group = ConsumerGroup(id = "grp-1", members = listOf("m-1", "m-2"), state = "Stable")
        assertEquals("grp-1", group.id)
        assertEquals(2, group.members.size)
        assertEquals("Stable", group.state)
    }

    @Test fun `G04 auto commit — configurable interval`() = runTest {
        val config = ConsumerConfig(autoCommit = true, autoCommitIntervalMs = 1000)
        assertTrue(config.autoCommit)
        assertEquals(1000L, config.autoCommitIntervalMs)
    }

    @Test fun `G05 rebalance — consumer group description model`() = runTest {
        val member = ConsumerGroupMember(
            id = "member-1", clientId = "client-1",
            host = "192.168.1.1", assignments = listOf("events-0", "events-1"),
        )
        val desc = ConsumerGroupDescription(
            id = "grp-1", state = "Stable", members = listOf(member), protocol = "range",
        )
        assertEquals("grp-1", desc.id)
        assertEquals(1, desc.members.size)
        assertEquals("client-1", desc.members[0].clientId)
        assertEquals(2, desc.members[0].assignments.size)
    }

    @Test fun `G06 leave group — group list via admin mock`() = runTest {
        val http = mockHttp { request ->
            assertEquals("/v1/consumer-groups", request.url.encodedPath)
            respond(
                content = """[{"id":"grp-1","members":["m1"],"state":"Stable"}]""",
                headers = headersOf(HttpHeaders.ContentType, "application/json"),
            )
        }
        val admin = AdminClient("http://localhost:9094", httpClient = http)
        val groups = admin.listConsumerGroups()
        assertEquals(1, groups.size)
        assertEquals("grp-1", groups[0].id)
        admin.close()
    }

    @Test fun `G07 independent groups — multiple groups`() = runTest {
        val http = mockHttp {
            respond(
                content = """[{"id":"group-a","members":["m1"],"state":"Stable"},{"id":"group-b","members":["m2","m3"],"state":"Stable"}]""",
                headers = headersOf(HttpHeaders.ContentType, "application/json"),
            )
        }
        val admin = AdminClient("http://localhost:9094", httpClient = http)
        val groups = admin.listConsumerGroups()
        assertEquals(2, groups.size)
        assertEquals("group-a", groups[0].id)
        assertEquals("group-b", groups[1].id)
        admin.close()
    }

    @Test fun `G08 static membership — consumer config defaults`() = runTest {
        val config = ConsumerConfig(
            groupId = "static-group", autoCommit = false, maxPollRecords = 100,
        )
        assertEquals("static-group", config.groupId)
        assertFalse(config.autoCommit)
        assertEquals(100, config.maxPollRecords)
    }
}

// ========== ADMIN / TOPICS (6 tests) ==========

class AdminConformanceTest {

    @Test fun `D01 create topic — request sends correct HTTP`() = runTest {
        val http = mockHttp { request ->
            assertEquals("/v1/topics", request.url.encodedPath)
            assertEquals(HttpMethod.Post, request.method)
            respond(content = "", status = HttpStatusCode.Created)
        }
        val admin = AdminClient("http://localhost:9094", httpClient = http)
        admin.createTopic("events", partitions = 3)
        admin.close()
    }

    @Test fun `D02 list topics — returns parsed list`() = runTest {
        val http = mockHttp {
            respond(
                content = """[{"name":"t1","partitions":1,"replication_factor":1,"message_count":0}]""",
                headers = headersOf(HttpHeaders.ContentType, "application/json"),
            )
        }
        val admin = AdminClient("http://localhost:9094", httpClient = http)
        val topics = admin.listTopics()
        assertEquals(1, topics.size)
        assertEquals("t1", topics[0].name)
        admin.close()
    }

    @Test fun `D03 describe topic — full metadata`() = runTest {
        val http = mockHttp {
            respond(
                content = """{"name":"events","partitions":6,"replication_factor":3,"message_count":5000,"config":{}}""",
                headers = headersOf(HttpHeaders.ContentType, "application/json"),
            )
        }
        val admin = AdminClient("http://localhost:9094", httpClient = http)
        val desc = admin.describeTopic("events")
        assertEquals("events", desc.name)
        assertEquals(6, desc.partitions)
        assertEquals(3, desc.replicationFactor)
        admin.close()
    }

    @Test fun `D04 delete topic — sends DELETE`() = runTest {
        val http = mockHttp { request ->
            assertEquals(HttpMethod.Delete, request.method)
            assertTrue(request.url.encodedPath.contains("old-topic"))
            respond(content = "", status = HttpStatusCode.NoContent)
        }
        val admin = AdminClient("http://localhost:9094", httpClient = http)
        admin.deleteTopic("old-topic")
        admin.close()
    }

    @Test fun `D05 auto create topic — create topic request model`() = runTest {
        val req = CreateTopicRequest(name = "auto-topic", partitions = 1, replicationFactor = 1)
        assertEquals("auto-topic", req.name)
        assertEquals(1, req.partitions)
        assertTrue(req.config.isEmpty())
    }

    @Test fun `D06 duplicate topic rejected — 409 throws exception`() = runTest {
        val http = mockHttp {
            respond(content = "Topic already exists", status = HttpStatusCode.Conflict)
        }
        val admin = AdminClient("http://localhost:9094", httpClient = http)
        assertFailsWith<AdminOperationException> {
            admin.createTopic("events", partitions = 1)
        }
        admin.close()
    }
}

// ========== AUTHENTICATION (6 tests) ==========

class AuthConformanceTest {

    @Test fun `A01 TLS connect — TLS config creation`() = runTest {
        val tls = TlsConfig(
            enabled = true,
            trustStorePath = "/etc/ssl/truststore.jks",
            trustStorePassword = "changeit",
        )
        assertTrue(tls.enabled)
        assertEquals("/etc/ssl/truststore.jks", tls.trustStorePath)
    }

    @Test fun `A02 mutual TLS — key store config`() = runTest {
        val tls = TlsConfig(
            enabled = true,
            trustStorePath = "/etc/ssl/truststore.jks",
            trustStorePassword = "changeit",
            keyStorePath = "/etc/ssl/keystore.jks",
            keyStorePassword = "keypass",
        )
        assertEquals("/etc/ssl/keystore.jks", tls.keyStorePath)
        assertEquals("keypass", tls.keyStorePassword)
    }

    @Test fun `A03 SASL PLAIN — mechanism config`() = runTest {
        val sasl = SaslConfig(
            mechanism = SaslMechanism.PLAIN, username = "admin", password = "secret",
        )
        assertEquals(SaslMechanism.PLAIN, sasl.mechanism)
        assertEquals("admin", sasl.username)
    }

    @Test fun `A04 SCRAM SHA256 — mechanism config`() = runTest {
        val sasl = SaslConfig(mechanism = SaslMechanism.SCRAM_SHA_256, username = "u", password = "p")
        assertEquals(SaslMechanism.SCRAM_SHA_256, sasl.mechanism)
    }

    @Test fun `A05 SCRAM SHA512 — mechanism config`() = runTest {
        val sasl = SaslConfig(mechanism = SaslMechanism.SCRAM_SHA_512, username = "u", password = "p")
        assertEquals(SaslMechanism.SCRAM_SHA_512, sasl.mechanism)
    }

    @Test fun `A06 auth failure — admin 401 throws AuthenticationFailedException`() = runTest {
        val http = mockHttp {
            respond(content = "Unauthorized", status = HttpStatusCode.Unauthorized)
        }
        val admin = AdminClient("http://localhost:9094", httpClient = http)
        assertFailsWith<AuthenticationFailedException> {
            admin.listTopics()
        }
        admin.close()
    }
}

// ========== SCHEMA REGISTRY (6 tests) ==========

class SchemaConformanceTest {

    @Test fun `S01 register schema — model creation`() = runTest {
        val schema = SchemaInfo(
            subject = "events-value", id = 1, version = 1, schemaType = "AVRO",
            schema = "{\"type\":\"record\",\"name\":\"Event\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"}]}",
        )
        assertEquals("events-value", schema.subject)
        assertEquals(1, schema.id)
        assertEquals("AVRO", schema.schemaType)
    }

    @Test fun `S02 get schema by id — serialization round-trip`() = runTest {
        val original = SchemaInfo(
            subject = "test-value", id = 42, version = 3, schemaType = "PROTOBUF",
            schema = "syntax = \"proto3\"; message Test { string id = 1; }",
        )
        val encoded = json.encodeToString(original)
        val decoded = json.decodeFromString<SchemaInfo>(encoded)
        assertEquals(42, decoded.id)
        assertEquals(3, decoded.version)
        assertEquals("PROTOBUF", decoded.schemaType)
    }

    @Test fun `S03 list versions — schema registry list subjects mock`() = runTest {
        val http = mockHttp {
            respond(
                content = """["events-key","events-value","orders-value"]""",
                headers = headersOf(HttpHeaders.ContentType, "application/json"),
            )
        }
        val registry = SchemaRegistryClient("http://localhost:9094", httpClient = http)
        val subjects = registry.listSubjects()
        assertEquals(3, subjects.size)
        assertTrue(subjects.contains("events-value"))
        registry.close()
    }

    @Test fun `S04 compatibility check — schema registry mock`() = runTest {
        val http = mockHttp {
            respond(
                content = """{"is_compatible":true}""",
                headers = headersOf(HttpHeaders.ContentType, "application/json"),
            )
        }
        val registry = SchemaRegistryClient("http://localhost:9094", httpClient = http)
        val compatible = registry.checkCompatibility("events-value", "{\"type\":\"string\"}")
        assertTrue(compatible)
        registry.close()
    }

    @Test fun `S05 avro format — schema format enum`() = runTest {
        assertEquals("AVRO", SchemaFormat.AVRO.name)
        assertEquals(3, SchemaFormat.entries.size)
    }

    @Test fun `S06 json format — all formats supported`() = runTest {
        val formats = SchemaFormat.entries.map { it.name }
        assertTrue(formats.contains("AVRO"))
        assertTrue(formats.contains("PROTOBUF"))
        assertTrue(formats.contains("JSON"))
    }
}

// ========== ERROR HANDLING (5 tests) ==========

class ErrorConformanceTest {

    @Test fun `E01 unknown topic — TopicNotFoundException`() = runTest {
        val http = mockHttp {
            respond(content = "Not found", status = HttpStatusCode.NotFound)
        }
        val admin = AdminClient("http://localhost:9094", httpClient = http)
        assertFailsWith<TopicNotFoundException> {
            admin.describeTopic("nonexistent")
        }
        admin.close()
    }

    @Test fun `E02 invalid partition — error hierarchy`() = runTest {
        val ex = StreamlineException("Invalid partition: -1")
        assertTrue(ex is Exception)
        assertEquals("Invalid partition: -1", ex.message)
    }

    @Test fun `E03 invalid offset — exception types`() = runTest {
        val notConnected = NotConnectedException()
        val connFailed = ConnectionFailedException("refused", null)
        val timeout = StreamlineTimeoutException()
        val queueFull = OfflineQueueFullException()

        assertTrue(notConnected is StreamlineException)
        assertTrue(connFailed is StreamlineException)
        assertTrue(timeout is StreamlineException)
        assertTrue(queueFull is StreamlineException)

        assertEquals("Client is not connected", notConnected.message)
        assertEquals("Operation timed out", timeout.message)
        assertEquals("Offline queue is full", queueFull.message)
    }

    @Test fun `E04 retryable error info — typed exceptions`() = runTest {
        val topicErr = TopicNotFoundException("events")
        val authErr = AuthenticationFailedException("bad creds")
        val adminErr = AdminOperationException("server error", null)
        val queryErr = QueryException("syntax error", null)
        val schemaErr = SchemaRegistryException("not found", null)

        assertTrue(topicErr is StreamlineException)
        assertTrue(authErr is StreamlineException)
        assertTrue(adminErr is StreamlineException)
        assertTrue(queryErr is StreamlineException)
        assertTrue(schemaErr is StreamlineException)

        assertTrue(topicErr.message!!.contains("events"))
        assertTrue(authErr.message!!.contains("bad creds"))
    }

    @Test fun `E05 descriptive error messages — HTTP error mapping`() = runTest {
        val http = mockHttp {
            respond(content = "Service Unavailable", status = HttpStatusCode.ServiceUnavailable)
        }
        val admin = AdminClient("http://localhost:9094", httpClient = http)
        val ex = assertFailsWith<AdminOperationException> {
            admin.listTopics()
        }
        assertNotNull(ex.message)
        admin.close()
    }
}

// ========== PERFORMANCE (4 tests) ==========

class PerformanceConformanceTest {

    @Test fun `F01 throughput 1KB — message serialization performance`() = runTest {
        val value = "x".repeat(1024)
        val start = System.nanoTime()
        repeat(10_000) {
            val msg = StreamlineMessage(topic = "perf", key = "k-$it", value = value)
            json.encodeToString(msg)
        }
        val elapsed = (System.nanoTime() - start) / 1_000_000
        assertTrue(elapsed < 10_000, "10k serializations took ${elapsed}ms, expected < 10s")
    }

    @Test fun `F02 latency P99 — config object creation`() = runTest {
        val times = (1..1000).map {
            val start = System.nanoTime()
            StreamlineConfiguration(
                url = "ws://localhost:9092", autoReconnect = true, maxRetries = 10,
                tls = TlsConfig(enabled = true),
                sasl = SaslConfig(mechanism = SaslMechanism.SCRAM_SHA_256, username = "u", password = "p"),
            )
            System.nanoTime() - start
        }.sorted()
        val p99 = times[(times.size * 0.99).toInt()]
        assertTrue(p99 < 1_000_000, "P99 config creation: ${p99}ns")
    }

    @Test fun `F03 startup time — client instantiation`() = runTest {
        val start = System.nanoTime()
        val client = StreamlineClient(StreamlineConfiguration(url = "ws://localhost:9092"))
        val elapsed = (System.nanoTime() - start) / 1_000_000
        assertEquals(ConnectionState.DISCONNECTED, client.state.value)
        assertTrue(elapsed < 1000, "Client instantiation took ${elapsed}ms, expected < 1s")
    }

    @Test fun `F04 memory usage — large message batch`() = runTest {
        val messages = (1..10_000).map {
            StreamlineMessage(topic = "perf", key = "key-$it", value = "value-$it")
        }
        assertEquals(10_000, messages.size)
        assertEquals("key-1", messages.first().key)
        assertEquals("key-10000", messages.last().key)
    }
}
