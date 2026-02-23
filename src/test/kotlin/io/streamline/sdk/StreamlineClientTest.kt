package io.streamline.sdk

import kotlinx.coroutines.flow.first
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class StreamlineClientTest {

    @Test
    fun `default configuration values`() {
        val config = StreamlineConfiguration(url = "ws://localhost:9092")
        assertEquals(true, config.autoReconnect)
        assertEquals(10, config.maxRetries)
        assertEquals(30_000, config.timeoutMs)
        assertNull(config.authToken)
    }

    @Test
    fun `initial state is disconnected`() = runTest {
        val client = StreamlineClient(StreamlineConfiguration(url = "ws://localhost:9092"))
        assertEquals(ConnectionState.DISCONNECTED, client.state.value)
        client.close()
    }

    @Test
    fun `message data class equality`() {
        val a = StreamlineMessage(topic = "t", key = "k", value = "v", offset = 1)
        val b = StreamlineMessage(topic = "t", key = "k", value = "v", offset = 1)
        assertEquals(a, b)
    }

    @Test
    fun `topic info serialization round-trip`() {
        val info = TopicInfo(name = "test", partitions = 3, replicationFactor = 1, messageCount = 42)
        assertEquals("test", info.name)
        assertEquals(3, info.partitions)
    }
}
