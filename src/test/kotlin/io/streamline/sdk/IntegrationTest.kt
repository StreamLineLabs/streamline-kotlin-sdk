package io.streamline.sdk

import kotlinx.coroutines.delay
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withTimeout
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import kotlin.test.fail
import kotlin.time.Duration.Companion.seconds

/**
 * Integration tests that run against a real Streamline server.
 *
 * Prerequisites:
 *   docker compose -f docker-compose.test.yml up -d
 *
 * These tests are skipped automatically when no server is reachable.
 * To run: ./gradlew test -Dstreamline.integration=true
 */
class IntegrationTest {

    companion object {
        private const val WS_URL = "ws://localhost:9092"
        private const val HTTP_URL = "http://localhost:9094"

        private val enabled: Boolean by lazy {
            System.getProperty("streamline.integration")?.toBoolean() == true
        }
    }

    private fun skipIfDisabled() {
        if (!enabled) {
            println("⏭ Integration tests disabled (set -Dstreamline.integration=true)")
            return
        }
    }

    // ── Admin Client ──────────────────────────────────────────────

    @Test
    fun `INT-01 admin health check`() = runTest {
        skipIfDisabled()
        if (!enabled) return@runTest

        val admin = AdminClient(HTTP_URL)
        try {
            val healthy = admin.isHealthy()
            assertTrue(healthy, "Server should be healthy")
        } finally {
            admin.close()
        }
    }

    @Test
    fun `INT-02 admin server info`() = runTest {
        skipIfDisabled()
        if (!enabled) return@runTest

        val admin = AdminClient(HTTP_URL)
        try {
            val info = admin.serverInfo()
            assertNotNull(info.version, "Version should not be null")
            assertTrue(info.version.isNotBlank(), "Version should not be blank")
        } finally {
            admin.close()
        }
    }

    @Test
    fun `INT-03 topic lifecycle — create, list, describe, delete`() = runTest {
        skipIfDisabled()
        if (!enabled) return@runTest

        val admin = AdminClient(HTTP_URL)
        val topicName = "int-test-topic-${System.currentTimeMillis()}"
        try {
            // Create
            admin.createTopic(topicName, partitions = 2, replicationFactor = 1)

            // List should contain our topic
            val topics = admin.listTopics()
            assertTrue(topics.any { it.name == topicName }, "Topic should appear in list")

            // Describe
            val desc = admin.describeTopic(topicName)
            assertEquals(topicName, desc.name)
            assertEquals(2, desc.partitions)

            // Delete
            admin.deleteTopic(topicName)

            // Verify deleted
            delay(500)
            val topicsAfter = admin.listTopics()
            assertTrue(topicsAfter.none { it.name == topicName }, "Topic should be deleted")
        } finally {
            admin.close()
        }
    }

    // ── Produce & Consume ─────────────────────────────────────────

    @Test
    fun `INT-04 produce and consume via WebSocket`() = runTest(timeout = 30.seconds) {
        skipIfDisabled()
        if (!enabled) return@runTest

        val admin = AdminClient(HTTP_URL)
        val topicName = "int-test-produce-${System.currentTimeMillis()}"
        admin.createTopic(topicName, partitions = 1)

        val config = StreamlineConfiguration(url = WS_URL, timeoutMs = 10_000)
        val client = StreamlineClient(config)
        try {
            client.connect()
            assertEquals(ConnectionState.CONNECTED, client.state.value)

            // Produce
            client.produce(topicName, key = "k1", value = "hello-integration")

            // Subscribe and verify receipt
            var received: StreamlineMessage? = null
            client.subscribe(topicName) { msg ->
                received = msg
            }

            // Wait for delivery
            withTimeout(10_000) {
                while (received == null) {
                    delay(100)
                }
            }

            assertNotNull(received)
            assertEquals(topicName, received!!.topic)
            assertEquals("hello-integration", received!!.value)
        } finally {
            client.disconnect()
            client.close()
            admin.deleteTopic(topicName)
            admin.close()
        }
    }

    // ── Poll-based Consumption ────────────────────────────────────

    @Test
    fun `INT-05 poll returns messages within timeout`() = runTest(timeout = 30.seconds) {
        skipIfDisabled()
        if (!enabled) return@runTest

        val admin = AdminClient(HTTP_URL)
        val topicName = "int-test-poll-${System.currentTimeMillis()}"
        admin.createTopic(topicName, partitions = 1)

        val config = StreamlineConfiguration(url = WS_URL, timeoutMs = 10_000)
        val client = StreamlineClient(config)
        client.consumerConfig = ConsumerConfig(maxPollRecords = 10)
        try {
            client.connect()

            // Produce several messages
            repeat(5) { i ->
                client.produce(topicName, key = "k$i", value = "poll-msg-$i")
            }
            client.flushBatch()

            // Subscribe (needed to start receiving)
            client.subscribe(topicName) { }

            delay(2000) // Allow messages to arrive

            val batch = client.poll(timeoutMs = 5000)
            assertTrue(batch.isNotEmpty(), "Poll should return messages")
            assertTrue(batch.size <= 10, "Should respect maxPollRecords")
        } finally {
            client.disconnect()
            client.close()
            admin.deleteTopic(topicName)
            admin.close()
        }
    }

    // ── Batch Produce ─────────────────────────────────────────────

    @Test
    fun `INT-06 batch produce returns delivery status`() = runTest(timeout = 30.seconds) {
        skipIfDisabled()
        if (!enabled) return@runTest

        val admin = AdminClient(HTTP_URL)
        val topicName = "int-test-batch-${System.currentTimeMillis()}"
        admin.createTopic(topicName, partitions = 1)

        val config = StreamlineConfiguration(url = WS_URL, timeoutMs = 10_000)
        val client = StreamlineClient(config)
        try {
            client.connect()

            val messages = (1..10).map { i ->
                StreamlineMessage(topic = topicName, key = "bk$i", value = "batch-val-$i")
            }
            val result = client.produceBatch(messages)

            assertEquals(10, result.successCount, "All messages should succeed")
            assertEquals(0, result.failureCount, "No failures expected")
            assertTrue(result.errors.isEmpty(), "No errors expected")
        } finally {
            client.disconnect()
            client.close()
            admin.deleteTopic(topicName)
            admin.close()
        }
    }

    // ── Transactions ──────────────────────────────────────────────

    @Test
    fun `INT-07 transaction commit sends buffered messages`() = runTest(timeout = 30.seconds) {
        skipIfDisabled()
        if (!enabled) return@runTest

        val admin = AdminClient(HTTP_URL)
        val topicName = "int-test-txn-${System.currentTimeMillis()}"
        admin.createTopic(topicName, partitions = 1)

        val config = StreamlineConfiguration(url = WS_URL, timeoutMs = 10_000)
        val client = StreamlineClient(config)
        try {
            client.connect()

            client.beginTransaction()
            client.transactionalProduce(topicName, key = "txn-k1", value = "txn-v1")
            client.transactionalProduce(topicName, key = "txn-k2", value = "txn-v2")
            client.commitTransaction()

            // Verify messages arrived
            var count = 0
            client.subscribe(topicName) { count++ }
            delay(3000)
            assertTrue(count >= 2, "Should receive transactional messages")
        } finally {
            client.disconnect()
            client.close()
            admin.deleteTopic(topicName)
            admin.close()
        }
    }

    @Test
    fun `INT-08 transaction abort discards buffered messages`() = runTest(timeout = 30.seconds) {
        skipIfDisabled()
        if (!enabled) return@runTest

        val config = StreamlineConfiguration(url = WS_URL, timeoutMs = 10_000)
        val client = StreamlineClient(config)
        try {
            client.connect()

            client.beginTransaction()
            client.transactionalProduce("some-topic", key = "k", value = "v")
            client.abortTransaction()

            // After abort, no transaction should be active
            // Attempting to commit should fail
            try {
                client.commitTransaction()
                fail("Should throw on commit without active transaction")
            } catch (e: StreamlineException) {
                assertEquals(ErrorCode.PROTOCOL, e.errorCode)
            }
        } finally {
            client.disconnect()
            client.close()
        }
    }

    // ── Client Metrics ────────────────────────────────────────────

    @Test
    fun `INT-09 metrics track produce and consume counts`() = runTest(timeout = 30.seconds) {
        skipIfDisabled()
        if (!enabled) return@runTest

        val admin = AdminClient(HTTP_URL)
        val topicName = "int-test-metrics-${System.currentTimeMillis()}"
        admin.createTopic(topicName, partitions = 1)

        val config = StreamlineConfiguration(url = WS_URL, timeoutMs = 10_000)
        val client = StreamlineClient(config)
        try {
            client.connect()

            // Produce
            repeat(5) { client.produce(topicName, value = "metric-msg-$it") }
            client.flushBatch()

            val snapshot = client.metrics
            assertTrue(snapshot.produceCount >= 5, "Should count produce calls")
            assertTrue(snapshot.produceBytes > 0, "Should track produce bytes")
            assertEquals(0L, snapshot.produceErrors, "No errors expected")
        } finally {
            client.disconnect()
            client.close()
            admin.deleteTopic(topicName)
            admin.close()
        }
    }

    // ── HTTP Offset Management ────────────────────────────────────

    @Test
    fun `INT-10 admin commit and fetch offsets via HTTP`() = runTest(timeout = 30.seconds) {
        skipIfDisabled()
        if (!enabled) return@runTest

        val admin = AdminClient(HTTP_URL)
        val groupId = "int-test-group-${System.currentTimeMillis()}"
        try {
            // Commit offsets
            admin.commitOffsets(groupId, mapOf("test-topic:0" to 42L, "test-topic:1" to 100L))

            // Fetch offsets
            val offsets = admin.getOffsets(groupId)
            assertTrue(offsets.isNotEmpty(), "Should return committed offsets")
        } finally {
            admin.close()
        }
    }

    // ── Consumer Group via Admin ──────────────────────────────────

    @Test
    fun `INT-11 list consumer groups`() = runTest {
        skipIfDisabled()
        if (!enabled) return@runTest

        val admin = AdminClient(HTTP_URL)
        try {
            // Should not throw
            val groups = admin.listConsumerGroups()
            assertNotNull(groups)
        } finally {
            admin.close()
        }
    }

    // ── Cluster Info ──────────────────────────────────────────────

    @Test
    fun `INT-12 cluster info returns broker list`() = runTest {
        skipIfDisabled()
        if (!enabled) return@runTest

        val admin = AdminClient(HTTP_URL)
        try {
            val cluster = admin.clusterInfo()
            assertTrue(cluster.brokers.isNotEmpty(), "Should have at least one broker")
        } finally {
            admin.close()
        }
    }
}
