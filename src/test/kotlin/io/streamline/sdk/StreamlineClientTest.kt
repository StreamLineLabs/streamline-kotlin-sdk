package io.streamline.sdk

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

class StreamlineClientTest {

    // -- Configuration --

    @Test
    fun `default configuration values`() {
        val config = StreamlineConfiguration(url = "ws://localhost:9092")
        assertEquals(true, config.autoReconnect)
        assertEquals(10, config.maxRetries)
        assertEquals(30_000, config.timeoutMs)
        assertNull(config.authToken)
        assertEquals(500, config.initialBackoffMs)
        assertEquals(30_000, config.maxBackoffMs)
    }

    @Test
    fun `custom configuration values`() {
        val config = StreamlineConfiguration(
            url = "ws://myhost:9092",
            autoReconnect = false,
            maxRetries = 5,
            timeoutMs = 10_000,
            authToken = "secret-token",
            initialBackoffMs = 1000,
            maxBackoffMs = 60_000,
        )
        assertEquals("ws://myhost:9092", config.url)
        assertFalse(config.autoReconnect)
        assertEquals(5, config.maxRetries)
        assertEquals(10_000, config.timeoutMs)
        assertEquals("secret-token", config.authToken)
        assertEquals(1000, config.initialBackoffMs)
        assertEquals(60_000, config.maxBackoffMs)
    }

    // -- Connection State --

    @Test
    fun `initial state is disconnected`() = runTest {
        val client = StreamlineClient(StreamlineConfiguration(url = "ws://localhost:9092"))
        assertEquals(ConnectionState.DISCONNECTED, client.state.value)
        client.close()
    }

    @Test
    fun `all connection states are distinct`() {
        val states = ConnectionState.entries
        assertEquals(4, states.size)
        assertTrue(states.contains(ConnectionState.DISCONNECTED))
        assertTrue(states.contains(ConnectionState.CONNECTING))
        assertTrue(states.contains(ConnectionState.CONNECTED))
        assertTrue(states.contains(ConnectionState.RECONNECTING))
    }

    // -- Message Model --

    @Test
    fun `message data class equality`() {
        val a = StreamlineMessage(topic = "t", key = "k", value = "v", offset = 1)
        val b = StreamlineMessage(topic = "t", key = "k", value = "v", offset = 1)
        assertEquals(a, b)
    }

    @Test
    fun `message with null key and offset`() {
        val msg = StreamlineMessage(topic = "test", value = "hello")
        assertNull(msg.key)
        assertNull(msg.offset)
        assertNull(msg.timestamp)
    }

    @Test
    fun `message serialization round trip`() {
        val json = Json { encodeDefaults = true }
        val original = StreamlineMessage(topic = "events", key = "k1", value = """{"x":1}""", offset = 42, timestamp = 1000L)
        val serialized = json.encodeToString(original)
        val deserialized = json.decodeFromString<StreamlineMessage>(serialized)
        assertEquals(original, deserialized)
    }

    @Test
    fun `message copy with modified fields`() {
        val msg = StreamlineMessage(topic = "t", value = "v")
        val copied = msg.copy(key = "new-key", offset = 100)
        assertEquals("new-key", copied.key)
        assertEquals(100L, copied.offset)
        assertEquals("t", copied.topic)
    }

    // -- Topic Info --

    @Test
    fun `topic info serialization round trip`() {
        val json = Json { encodeDefaults = true }
        val info = TopicInfo(name = "test", partitions = 3, replicationFactor = 1, messageCount = 42)
        val serialized = json.encodeToString(info)
        val deserialized = json.decodeFromString<TopicInfo>(serialized)
        assertEquals(info, deserialized)
    }

    @Test
    fun `topic description with config`() {
        val desc = TopicDescription(
            name = "events",
            partitions = 6,
            replicationFactor = 3,
            messageCount = 1000,
            config = mapOf("retention.ms" to "86400000", "cleanup.policy" to "delete"),
        )
        assertEquals("events", desc.name)
        assertEquals(2, desc.config.size)
        assertEquals("86400000", desc.config["retention.ms"])
    }

    @Test
    fun `topic description default config is empty`() {
        val desc = TopicDescription(name = "t", partitions = 1, replicationFactor = 1)
        assertTrue(desc.config.isEmpty())
        assertEquals(0L, desc.messageCount)
    }

    // -- Consumer Group --

    @Test
    fun `consumer group serialization`() {
        val json = Json { encodeDefaults = true }
        val group = ConsumerGroup(id = "my-group", members = listOf("m1", "m2"), state = "Stable")
        val serialized = json.encodeToString(group)
        val deserialized = json.decodeFromString<ConsumerGroup>(serialized)
        assertEquals(group, deserialized)
    }

    @Test
    fun `consumer group description with members`() {
        val desc = ConsumerGroupDescription(
            id = "cg-1",
            state = "Stable",
            members = listOf(
                ConsumerGroupMember(id = "m1", clientId = "client-1", host = "10.0.0.1", assignments = listOf("events-0", "events-1")),
                ConsumerGroupMember(id = "m2", clientId = "client-2", host = "10.0.0.2", assignments = listOf("events-2")),
            ),
            protocol = "range",
        )
        assertEquals(2, desc.members.size)
        assertEquals("client-1", desc.members[0].clientId)
        assertEquals(2, desc.members[0].assignments.size)
    }

    @Test
    fun `consumer group member defaults`() {
        val member = ConsumerGroupMember(id = "m1")
        assertEquals("", member.clientId)
        assertEquals("", member.host)
        assertTrue(member.assignments.isEmpty())
    }

    // -- Query Result --

    @Test
    fun `query result with data`() {
        val result = QueryResult(
            columns = listOf("key", "value", "offset"),
            rows = listOf(
                listOf("k1", "v1", "0"),
                listOf("k2", "v2", "1"),
            ),
            rowCount = 2,
        )
        assertEquals(3, result.columns.size)
        assertEquals(2, result.rows.size)
        assertEquals("k1", result.rows[0][0])
    }

    @Test
    fun `query result empty defaults`() {
        val result = QueryResult()
        assertTrue(result.columns.isEmpty())
        assertTrue(result.rows.isEmpty())
        assertEquals(0, result.rowCount)
    }

    // -- Server Info --

    @Test
    fun `server info fields`() {
        val info = ServerInfo(version = "0.2.0", uptime = 3600, topicCount = 10, messageCount = 50000)
        assertEquals("0.2.0", info.version)
        assertEquals(3600L, info.uptime)
        assertEquals(10, info.topicCount)
        assertEquals(50000L, info.messageCount)
    }

    @Test
    fun `server info defaults`() {
        val info = ServerInfo()
        assertEquals("", info.version)
        assertEquals(0L, info.uptime)
    }

    // -- Create Topic Request --

    @Test
    fun `create topic request defaults`() {
        val req = CreateTopicRequest(name = "my-topic")
        assertEquals("my-topic", req.name)
        assertEquals(1, req.partitions)
        assertEquals(1, req.replicationFactor)
        assertTrue(req.config.isEmpty())
    }

    @Test
    fun `create topic request with config`() {
        val req = CreateTopicRequest(
            name = "events",
            partitions = 12,
            replicationFactor = 3,
            config = mapOf("retention.ms" to "604800000"),
        )
        assertEquals(12, req.partitions)
        assertEquals("604800000", req.config["retention.ms"])
    }

    // -- Exception Hierarchy --

    @Test
    fun `exception hierarchy`() {
        val base = StreamlineException("base")
        val notConnected = NotConnectedException()
        val connFailed = ConnectionFailedException("fail", RuntimeException("cause"))
        val authFailed = AuthenticationFailedException("bad token")
        val timeout = StreamlineTimeoutException()
        val notFound = TopicNotFoundException("missing")
        val queueFull = OfflineQueueFullException()
        val adminOp = AdminOperationException("admin fail")
        val queryEx = QueryException("query fail")

        assertTrue(notConnected is StreamlineException)
        assertTrue(connFailed is StreamlineException)
        assertTrue(authFailed is StreamlineException)
        assertTrue(timeout is StreamlineException)
        assertTrue(notFound is StreamlineException)
        assertTrue(queueFull is StreamlineException)
        assertTrue(adminOp is StreamlineException)
        assertTrue(queryEx is StreamlineException)
        assertNotNull(connFailed.cause)
        assertEquals("Client is not connected", notConnected.message)
        assertEquals("Topic not found: missing", notFound.message)
    }

    @Test
    fun `admin operation exception with cause`() {
        val cause = RuntimeException("network error")
        val ex = AdminOperationException("request failed", cause)
        assertEquals("request failed", ex.message)
        assertEquals(cause, ex.cause)
    }

    @Test
    fun `query exception preserves message`() {
        val ex = QueryException("invalid SQL")
        assertEquals("invalid SQL", ex.message)
        assertNull(ex.cause)
    }
}

