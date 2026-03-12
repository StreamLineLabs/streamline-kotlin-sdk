package io.streamline.sdk

import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class OffsetManagementTest {

    private fun disconnectedClient(): StreamlineClient {
        return StreamlineClient(StreamlineConfiguration(url = "ws://localhost:9092"))
    }

    // -- commitOffsets --

    @Test
    fun `commitOffsets throws NotConnectedException when disconnected`() = runTest {
        val client = disconnectedClient()
        val ex = assertFailsWith<NotConnectedException> {
            client.commitOffsets(mapOf("events:0" to 42L))
        }
        assertEquals(ErrorCode.CONNECTION, ex.errorCode)
        assertTrue(ex.isRetryable())
        client.close()
    }

    @Test
    fun `commitOffsets throws NotConnectedException with empty offsets when disconnected`() = runTest {
        val client = disconnectedClient()
        assertFailsWith<NotConnectedException> {
            client.commitOffsets(emptyMap())
        }
        client.close()
    }

    // -- seekToOffset --

    @Test
    fun `seekToOffset throws NotConnectedException when disconnected`() = runTest {
        val client = disconnectedClient()
        val ex = assertFailsWith<NotConnectedException> {
            client.seekToOffset("events", 0, 100L)
        }
        assertEquals(ErrorCode.CONNECTION, ex.errorCode)
        client.close()
    }

    // -- seekToBeginning --

    @Test
    fun `seekToBeginning throws NotConnectedException when disconnected`() = runTest {
        val client = disconnectedClient()
        assertFailsWith<NotConnectedException> {
            client.seekToBeginning("events")
        }
        client.close()
    }

    // -- seekToEnd --

    @Test
    fun `seekToEnd throws NotConnectedException when disconnected`() = runTest {
        val client = disconnectedClient()
        assertFailsWith<NotConnectedException> {
            client.seekToEnd("events")
        }
        client.close()
    }

    // -- position --

    @Test
    fun `position throws NotConnectedException when disconnected`() = runTest {
        val client = disconnectedClient()
        assertFailsWith<NotConnectedException> {
            client.position("events", 0)
        }
        client.close()
    }

    // -- committed --

    @Test
    fun `committed throws NotConnectedException when disconnected`() = runTest {
        val client = disconnectedClient()
        assertFailsWith<NotConnectedException> {
            client.committed("events", 0)
        }
        client.close()
    }
}
