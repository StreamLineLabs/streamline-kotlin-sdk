package io.streamline.sdk

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.long
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

    @Test
    fun `produce payload safely serializes hostile input`() = runTest {
        val client = StreamlineClient(StreamlineConfiguration(url = "ws://localhost:9092"))
        val message = StreamlineMessage(
            topic = "events",
            key = "key\"\\\n\u0001",
            value = "\"},\"acks\":\"all\",\"admin\":true,\"value\":\"\n雪",
            headers = mapOf(
                "header\"}\n" to "value\\\"\r\u0002",
            ),
        )

        val payload = client.buildPayload(message)
        val decoded = Json.decodeFromString<ProduceCommand>(payload)
        val jsonObject = Json.parseToJsonElement(payload).jsonObject

        assertEquals(message.topic, decoded.topic)
        assertEquals(message.key, decoded.key)
        assertEquals(message.value, decoded.value)
        assertEquals(message.headers, decoded.headers)
        assertEquals("0", decoded.acks)
        assertFalse("admin" in jsonObject)
        client.close()
    }

    @Test
    fun `produce payload preserves optional producer protocol fields`() = runTest {
        val client = StreamlineClient(StreamlineConfiguration(url = "ws://localhost:9092"))
        client.producerConfig = ProducerConfig(compression = CompressionType.GZIP)

        val decoded = Json.decodeFromString<ProduceCommand>(
            client.buildPayload(StreamlineMessage(topic = "events", value = "payload", partition = 2)),
        )

        assertNull(decoded.key)
        assertEquals("0", decoded.acks)
        assertEquals("gzip", decoded.compression)
        assertNull(decoded.idempotent)
        assertNull(decoded.sequence)
        assertNull(decoded.headers)
        // An explicit partition must be preserved, never silently dropped.
        assertEquals(2, decoded.partition)
        client.close()
    }

    @Test
    fun `produce payload rejects an ack contract the transport cannot correlate`() = runTest {
        val client = StreamlineClient(StreamlineConfiguration(url = "ws://localhost:9092"))
        assertFailsWith<ConfigurationException> {
            client.producerConfig = ProducerConfig(
                compression = CompressionType.GZIP,
                idempotent = true,
                acks = Acks.ALL,
            )
        }
        client.close()
    }

    @Test
    fun `produce payload rejects a negative explicit partition instead of silently dropping it`() = runTest {
        val client = StreamlineClient(StreamlineConfiguration(url = "ws://localhost:9092"))
        assertFailsWith<IllegalArgumentException> {
            client.buildPayload(StreamlineMessage(topic = "events", value = "payload", partition = -1))
        }
        client.close()
    }

    @Test
    fun `single inbound dispatcher separates messages from control responses`() = runTest {
        val client = StreamlineClient(StreamlineConfiguration(url = "ws://localhost:9092"))
        val received = CompletableDeferred<StreamlineMessage>()
        client.subscribe("events") { received.complete(it) }
        val pendingResponse = client.registerControlResponse("expected-request")
        val message = StreamlineMessage(
            topic = "events",
            value = "payload",
            partition = 0,
            offset = 10,
        )

        client.dispatchIncoming(Json.encodeToString(message))

        assertEquals(message, withTimeout(1000) { received.await() })
        assertFalse(pendingResponse.response.isCompleted)

        client.dispatchIncoming("""{"status":"ok"}""")
        client.dispatchIncoming("""{"request_id":"another-request","status":"ok"}""")

        assertFalse(pendingResponse.response.isCompleted)

        val matchingResponse = """{"request_id":"expected-request","status":"ok"}"""
        client.dispatchIncoming(matchingResponse)

        assertEquals(matchingResponse, pendingResponse.response.await())
        client.close()
    }

    @Test
    fun `control commands include exact request correlation id`() {
        val client = StreamlineClient(StreamlineConfiguration(url = "ws://localhost:9092"))

        val command = Json.parseToJsonElement(
            client.addRequestId("""{"action":"position","partition":0}""", "request-123")
        ).jsonObject

        assertEquals("position", command["action"]?.toString()?.trim('"'))
        assertEquals("request-123", command["request_id"]?.toString()?.trim('"'))
        client.close()
    }

    @Test
    fun `commitOffsets payload safely serializes hostile topic-partition keys`() {
        val client = StreamlineClient(StreamlineConfiguration(url = "ws://localhost:9092"))

        val hostileKey = "events\"},\"admin\":true,\"x\":\"0"
        val command = client.buildCommitOffsetsCommand(mapOf(hostileKey to 5L))
        val jsonObject = Json.parseToJsonElement(command).jsonObject

        assertEquals("commit_offsets", jsonObject["action"]?.jsonPrimitive?.content)
        assertFalse("admin" in jsonObject)
        val offsets = jsonObject["offsets"]!!.jsonArray
        assertEquals(1, offsets.size)
        assertEquals(hostileKey, offsets[0].jsonObject["topicPartition"]?.jsonPrimitive?.content)
        assertEquals(5L, offsets[0].jsonObject["offset"]?.jsonPrimitive?.long)
        client.close()
    }

    @Test
    fun `unsubscribe rejects invalid topic name`() = runTest {
        val client = StreamlineClient(StreamlineConfiguration(url = "ws://localhost:9092"))
        assertFailsWith<IllegalArgumentException> { client.unsubscribe("bad topic!") }
        client.close()
    }

    @Test
    fun `seekToOffset rejects invalid topic name`() = runTest {
        val client = StreamlineClient(StreamlineConfiguration(url = "ws://localhost:9092"))
        assertFailsWith<IllegalArgumentException> { client.seekToOffset("bad topic!", 0, 0) }
        client.close()
    }

    @Test
    fun `seekToBeginning rejects invalid topic name`() = runTest {
        val client = StreamlineClient(StreamlineConfiguration(url = "ws://localhost:9092"))
        assertFailsWith<IllegalArgumentException> { client.seekToBeginning("bad topic!") }
        client.close()
    }

    @Test
    fun `seekToEnd rejects invalid topic name`() = runTest {
        val client = StreamlineClient(StreamlineConfiguration(url = "ws://localhost:9092"))
        assertFailsWith<IllegalArgumentException> { client.seekToEnd("bad topic!") }
        client.close()
    }

    @Test
    fun `position rejects invalid topic name`() = runTest {
        val client = StreamlineClient(StreamlineConfiguration(url = "ws://localhost:9092"))
        assertFailsWith<IllegalArgumentException> { client.position("bad topic!", 0) }
        client.close()
    }

    @Test
    fun `committed rejects invalid topic name`() = runTest {
        val client = StreamlineClient(StreamlineConfiguration(url = "ws://localhost:9092"))
        assertFailsWith<IllegalArgumentException> { client.committed("bad topic!", 0) }
        client.close()
    }

    @Test
    fun `transactionalProduce rejects invalid topic name`() = runTest {
        val client = StreamlineClient(StreamlineConfiguration(url = "ws://localhost:9092"))
        assertFailsWith<IllegalArgumentException> {
            client.transactionalProduce("bad topic!", value = "v")
        }
        client.close()
    }

    @Test
    fun `subscription handler suspension does not block inbound dispatcher`() = runTest {
        val client = StreamlineClient(StreamlineConfiguration(url = "ws://localhost:9092"))
        val handlerStarted = CompletableDeferred<Unit>()
        val releaseHandler = CompletableDeferred<Unit>()
        client.subscribe("events") {
            handlerStarted.complete(Unit)
            releaseHandler.await()
        }

        withTimeout(1000) {
            client.dispatchIncoming(
                Json.encodeToString(StreamlineMessage(topic = "events", value = "payload"))
            )
        }
        withTimeout(1000) {
            handlerStarted.await()
        }

        releaseHandler.complete(Unit)
        client.close()
    }

    @Test
    fun `handler cancellation does not stop later subscription delivery`() = runTest {
        val client = StreamlineClient(StreamlineConfiguration(url = "ws://localhost:9092"))
        val secondDelivery = CompletableDeferred<Unit>()
        var deliveryCount = 0
        client.subscribe("events") {
            deliveryCount++
            if (deliveryCount == 1) {
                throw CancellationException("handler timeout")
            }
            secondDelivery.complete(Unit)
        }

        client.dispatchIncoming(
            Json.encodeToString(StreamlineMessage(topic = "events", value = "first"))
        )
        client.dispatchIncoming(
            Json.encodeToString(StreamlineMessage(topic = "events", value = "second"))
        )

        withTimeout(1000) {
            secondDelivery.await()
        }
        assertEquals(2, deliveryCount)
        client.close()
    }

    @Test
    fun `close fails pending control response immediately`() = runTest {
        val client = StreamlineClient(StreamlineConfiguration(url = "ws://localhost:9092"))
        val pending = client.registerControlResponse("pending-request")

        client.close()

        assertFailsWith<NotConnectedException> {
            pending.response.await()
        }
    }

    // -- flushBatch: no loss / no duplication --

    @Test
    fun `flushBatch requeues all pending messages when the session is missing`() = runTest {
        val client = StreamlineClient(StreamlineConfiguration(url = "ws://localhost:9092"))
        val queued = listOf(
            StreamlineMessage(topic = "events", value = "a"),
            StreamlineMessage(topic = "events", value = "b"),
            StreamlineMessage(topic = "events", value = "c"),
        )
        client.seedPendingBatch(queued)

        // No connection was ever established, so wsSession is null: this is
        // the exact "missing session" race a dropped connection produces
        // between a batched produce() and a later flush.
        client.flushBatch()

        assertEquals(queued, client.pendingBatch(), "messages must be preserved, not dropped")
        client.close()
    }

    @Test
    fun `flushBatch does not duplicate or drop when a send fails`() = runTest {
        val client = StreamlineClient(StreamlineConfiguration(url = "ws://localhost:9092"))
        val messages = listOf(
            StreamlineMessage(topic = "events", value = "1"),
            StreamlineMessage(topic = "events", value = "2"),
            StreamlineMessage(topic = "events", value = "3"),
            StreamlineMessage(topic = "events", value = "4"),
        )
        val sent = mutableListOf<StreamlineMessage>()

        val thrown = assertFailsWith<ConnectionFailedException> {
            client.sendBatchOrRequeue(messages) { message ->
                // First two messages succeed; the third fails permanently
                // (as sendWithRetry does once retries are exhausted).
                if (sent.size == 2) throw ConnectionFailedException("boom")
                sent.add(message)
            }
        }
        assertEquals("boom", thrown.message)

        // Already-sent messages must never be resent...
        assertEquals(messages.take(2), sent)
        // ...and the failed message plus everything after it must be
        // requeued, in original order, exactly once.
        assertEquals(messages.drop(2), client.pendingBatch())
        client.close()
    }

    @Test
    fun `flushBatch requeue does not resend messages already confirmed sent`() = runTest {
        val client = StreamlineClient(StreamlineConfiguration(url = "ws://localhost:9092"))
        val messages = listOf(
            StreamlineMessage(topic = "events", value = "1"),
            StreamlineMessage(topic = "events", value = "2"),
        )
        var attempts = 0

        client.sendBatchOrRequeue(messages) { attempts++ }

        assertEquals(2, attempts, "every message should be sent exactly once on the happy path")
        assertTrue(client.pendingBatch().isEmpty(), "nothing should be requeued when all sends succeed")
        client.close()
    }

    // -- produceBatch: partition & ack-contract validation --

    @Test
    fun `produceBatch rejects a negative explicit partition before sending anything`() = runTest {
        val client = StreamlineClient(StreamlineConfiguration(url = "ws://localhost:9092"))
        assertFailsWith<IllegalArgumentException> {
            client.produceBatch(
                listOf(
                    StreamlineMessage(topic = "events", value = "ok", partition = 0),
                    StreamlineMessage(topic = "events", value = "bad", partition = -1),
                ),
            )
        }
        client.close()
    }

    @Test
    fun `produceBatch validates every message before sending any, even while disconnected`() = runTest {
        val client = StreamlineClient(StreamlineConfiguration(url = "ws://localhost:9092"))
        // No connect() was ever called (wsSession is null), yet the
        // partition-validation failure must still surface -- proving
        // validation runs for the whole batch before the session is even
        // consulted, so a single invalid entry can never cause a partial,
        // silently-inconsistent send.
        assertFailsWith<IllegalArgumentException> {
            client.produceBatch(listOf(StreamlineMessage(topic = "events", value = "bad", partition = -5)))
        }
        client.close()
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

        assertEquals(ErrorCode.CONNECTION, notConnected.errorCode)
        assertEquals(ErrorCode.CONNECTION, connFailed.errorCode)
        assertEquals(ErrorCode.AUTHENTICATION, authFailed.errorCode)
        assertEquals(ErrorCode.TIMEOUT, timeout.errorCode)
        assertEquals(ErrorCode.TOPIC_NOT_FOUND, notFound.errorCode)
        assertEquals(ErrorCode.INTERNAL, queueFull.errorCode)
        assertEquals(ErrorCode.INTERNAL, adminOp.errorCode)
        assertEquals(ErrorCode.INTERNAL, queryEx.errorCode)
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

    // -- Schema Pipeline --

    @Test
    fun `produceWithSchema throws when no schema registry configured`() = runTest {
        val client = StreamlineClient(StreamlineConfiguration(url = "ws://localhost:9092"))
        assertFailsWith<IllegalStateException> {
            client.produceWithSchema("events", key = "k1", value = "{}")
        }
        client.close()
    }

    // -- Consumer Config --

    @Test
    fun `consumer config defaults`() {
        val config = ConsumerConfig()
        assertNull(config.groupId)
        assertTrue(config.autoCommit)
        assertEquals(5000L, config.autoCommitIntervalMs)
        assertEquals(30000L, config.sessionTimeoutMs)
        assertEquals(3000L, config.heartbeatIntervalMs)
        assertEquals(500, config.maxPollRecords)
        assertEquals(OffsetReset.LATEST, config.autoOffsetReset)
    }

    @Test
    fun `consumer config custom values`() {
        val config = ConsumerConfig(
            groupId = "my-group",
            autoCommit = false,
            autoCommitIntervalMs = 10_000,
            autoOffsetReset = OffsetReset.EARLIEST,
            maxPollRecords = 100,
        )
        assertEquals("my-group", config.groupId)
        assertFalse(config.autoCommit)
        assertEquals(10_000L, config.autoCommitIntervalMs)
        assertEquals(OffsetReset.EARLIEST, config.autoOffsetReset)
        assertEquals(100, config.maxPollRecords)
    }

    // -- Producer Config --

    @Test
    fun `producer config defaults`() {
        val config = ProducerConfig()
        assertEquals(16384, config.batchSize)
        assertEquals(0L, config.lingerMs)
        assertEquals(CompressionType.NONE, config.compression)
        assertEquals(3, config.retries)
        assertEquals(100L, config.retryBackoffMs)
        assertFalse(config.idempotent)
        assertEquals(Acks.NONE, config.acks)
        // The default configuration must always be honorable end-to-end.
        config.validate()
    }

    @Test
    fun `producer config rejects Acks other than NONE`() {
        assertFailsWith<ConfigurationException> { ProducerConfig(acks = Acks.ONE).validate() }
        assertFailsWith<ConfigurationException> { ProducerConfig(acks = Acks.ALL).validate() }
    }

    @Test
    fun `producer config rejects idempotent production`() {
        assertFailsWith<ConfigurationException> { ProducerConfig(idempotent = true).validate() }
    }

    @Test
    fun `assigning an unsupported ack contract to a client is rejected immediately`() = runTest {
        val client = StreamlineClient(StreamlineConfiguration(url = "ws://localhost:9092"))
        assertFailsWith<ConfigurationException> {
            client.producerConfig = ProducerConfig(acks = Acks.ALL)
        }
        assertFailsWith<ConfigurationException> {
            client.producerConfig = ProducerConfig(idempotent = true)
        }
        // The previously-valid config must remain in effect after a rejected assignment.
        assertEquals(Acks.NONE, client.producerConfig.acks)
        assertFalse(client.producerConfig.idempotent)
        client.close()
    }

    // -- ACL Models --

    @Test
    fun `acl entry data class`() {
        val entry = AclEntry(
            principal = "User:alice",
            resourceType = "topic",
            resourceName = "events",
            operation = "read",
            permission = "allow",
            host = "10.0.0.1",
        )
        assertEquals("User:alice", entry.principal)
        assertEquals("10.0.0.1", entry.host)
    }

    @Test
    fun `acl entry default host is wildcard`() {
        val entry = AclEntry(
            principal = "User:bob",
            resourceType = "group",
            resourceName = "cg-1",
            operation = "read",
            permission = "allow",
        )
        assertEquals("*", entry.host)
    }

    @Test
    fun `acl resource types`() {
        val types = AclResourceType.entries
        assertEquals(4, types.size)
        assertTrue(types.contains(AclResourceType.TOPIC))
        assertTrue(types.contains(AclResourceType.GROUP))
        assertTrue(types.contains(AclResourceType.CLUSTER))
        assertTrue(types.contains(AclResourceType.TRANSACTIONAL_ID))
    }

    @Test
    fun `acl operations`() {
        val ops = AclOperation.entries
        assertEquals(7, ops.size)
        assertTrue(ops.contains(AclOperation.READ))
        assertTrue(ops.contains(AclOperation.WRITE))
        assertTrue(ops.contains(AclOperation.ALL))
    }

    @Test
    fun `acl permissions`() {
        val perms = AclPermission.entries
        assertEquals(2, perms.size)
        assertTrue(perms.contains(AclPermission.ALLOW))
        assertTrue(perms.contains(AclPermission.DENY))
    }

    // -- Message Metadata --

    @Test
    fun `message with partition and headers`() {
        val msg = StreamlineMessage(
            topic = "events",
            key = "k1",
            value = "v1",
            partition = 2,
            offset = 100,
            timestamp = 1234567890L,
            headers = mapOf("trace-id" to "abc-123"),
        )
        assertEquals(2, msg.partition)
        assertEquals("abc-123", msg.headers["trace-id"])
    }

    @Test
    fun `message defaults for new fields`() {
        val msg = StreamlineMessage(topic = "t", value = "v")
        assertNull(msg.partition)
        assertTrue(msg.headers.isEmpty())
    }
}
