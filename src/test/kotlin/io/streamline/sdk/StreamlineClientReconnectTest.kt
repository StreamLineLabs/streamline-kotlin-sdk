package io.streamline.sdk

import java.util.concurrent.CountDownLatch
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * Exercises [StreamlineClient] reconnect behavior against a real (in-process,
 * loopback-only) WebSocket server: subscription replay after a reconnect,
 * treating a clean server-initiated close as a disconnect that triggers
 * reconnection, and the connection-generation guard that keeps a stale
 * loop/reconnect from mutating state once a newer connection exists.
 */
class StreamlineClientReconnectTest {

    private lateinit var server: TestWebSocketServer

    @BeforeTest
    fun startServer() {
        server = TestWebSocketServer()
        server.start()
    }

    @AfterTest
    fun stopServer() {
        server.stop()
    }

    private fun fastReconnectConfig() = StreamlineConfiguration(
        url = server.wsUrl,
        autoReconnect = true,
        maxRetries = 10,
        initialBackoffMs = 20,
        maxBackoffMs = 100,
        timeoutMs = 5_000,
    )

    private suspend fun awaitCondition(timeoutMs: Long = 10_000, condition: () -> Boolean) {
        withTimeout(timeoutMs) {
            while (!condition()) delay(10)
        }
    }

    @Test
    fun `reconnect replays active subscriptions on the new session`() = runBlocking {
        val client = StreamlineClient(fastReconnectConfig())
        try {
            client.connect()
            client.subscribe("orders") { }
            awaitCondition { server.countAction("subscribe") >= 1 }

            // Force a clean, server-initiated close of the now-established
            // connection and wait for the client to reconnect.
            server.closeCurrentConnection()
            awaitCondition { client.state.value == ConnectionState.CONNECTED && server.countAction("subscribe") >= 2 }

            assertEquals(2, server.countAction("subscribe"), "subscribe must be replayed exactly once after reconnect")
            assertTrue(server.receivedFrames.all { """"topic":"orders"""" in it })
        } finally {
            client.disconnect()
            client.close()
        }
    }

    @Test
    fun `a clean server close is treated as a disconnect and triggers reconnect`() = runBlocking {
        val client = StreamlineClient(fastReconnectConfig())
        try {
            client.connect()
            assertEquals(ConnectionState.CONNECTED, client.state.value)

            server.closeCurrentConnection()

            // With the fix, a clean close (no exception, the incoming loop
            // just completes) must still flip through RECONNECTING and land
            // back on CONNECTED — not silently stay CONNECTED with a dead
            // session forever.
            awaitCondition { client.state.value == ConnectionState.RECONNECTING }
            awaitCondition { client.state.value == ConnectionState.CONNECTED }
        } finally {
            client.disconnect()
            client.close()
        }
    }

    @Test
    fun `generation advances exactly once per successful reconnect, guarding stale callbacks`() = runBlocking {
        val client = StreamlineClient(fastReconnectConfig())
        try {
            client.connect()
            val initialGeneration = client.currentGeneration()
            assertEquals(1L, initialGeneration)

            server.closeCurrentConnection()
            awaitCondition {
                client.currentGeneration() != initialGeneration && client.state.value == ConnectionState.CONNECTED
            }

            // Exactly one new session was established for the one forced
            // disconnect: a stale reconnect/loop firing again would have
            // pushed this past initialGeneration + 1.
            assertEquals(initialGeneration + 1, client.currentGeneration())

            // Give any (incorrectly) duplicated reconnect attempt a chance to
            // run before asserting it did not happen.
            delay(300)
            assertEquals(initialGeneration + 1, client.currentGeneration())
        } finally {
            client.disconnect()
            client.close()
        }
    }

    @Test
    fun `manual disconnect invalidates a concurrently scheduled stale reconnect`() = runBlocking {
        // Use a longer backoff so we can call disconnect() while the
        // automatic reconnect is still pending, and confirm it never fires.
        val config = StreamlineConfiguration(
            url = server.wsUrl,
            autoReconnect = true,
            maxRetries = 10,
            initialBackoffMs = 300,
            maxBackoffMs = 1_000,
            timeoutMs = 5_000,
        )
        val client = StreamlineClient(config)
        client.connect()
        assertEquals(ConnectionState.CONNECTED, client.state.value)

        server.closeCurrentConnection()
        awaitCondition(5_000) { client.state.value == ConnectionState.RECONNECTING }

        client.disconnect()

        // The scheduled reconnect's backoff (300ms) elapses here; it must
        // not resurrect the connection since disconnect() already
        // invalidated its generation.
        delay(600)

        assertEquals(ConnectionState.DISCONNECTED, client.state.value)
        client.close()
    }

    @Test
    fun `a handshake that completes after generation moved on is fenced off, not installed`() = runBlocking {
        val client = StreamlineClient(fastReconnectConfig())
        try {
            client.connect()
            assertEquals(ConnectionState.CONNECTED, client.state.value)
            val staleGeneration = client.currentGeneration()

            // Simulate a second handshake that was already in flight (e.g. a
            // slow network round-trip) when a disconnect() bumped the
            // generation, and only resolves afterwards.
            val lateSession = client.openRawSessionForTest()

            client.disconnect()
            assertEquals(ConnectionState.DISCONNECTED, client.state.value)

            // The late handshake now "completes": attempt to install it
            // using the generation it was reserved under, which disconnect()
            // has since made stale.
            val installed = client.installIfCurrent(staleGeneration, lateSession)

            assertFalse(installed, "a late handshake must be fenced off, not installed")
            assertEquals(
                ConnectionState.DISCONNECTED,
                client.state.value,
                "a fenced-off session must not resurrect connection state",
            )
            assertNull(client.currentSessionForTest(), "a fenced-off session must never become the active session")

            // The orphaned session itself must have been closed, not leaked.
            withTimeout(2_000) {
                assertNotNull(lateSession.closeReason.await())
            }
        } finally {
            client.disconnect()
            client.close()
        }
    }

    @Test
    fun `disconnect transition cannot interleave between generation check and session install`() =
        assertTeardownWinsInstallRace { it.disconnect() }

    @Test
    fun `close transition cannot interleave between generation check and session install`() =
        assertTeardownWinsInstallRace { it.close() }

    private fun assertTeardownWinsInstallRace(
        teardown: suspend (StreamlineClient) -> Unit,
    ) = runBlocking {
        val client = StreamlineClient(fastReconnectConfig())
        val installEntered = CountDownLatch(1)
        val releaseInstall = CountDownLatch(1)
        try {
            client.connect()
            val generation = client.currentGeneration()
            val session = assertNotNull(client.currentSessionForTest())

            val install =
                async(Dispatchers.IO) {
                    client.installIfCurrent(generation, session) {
                        installEntered.countDown()
                        releaseInstall.await()
                    }
                }
            withTimeout(2_000) {
                while (installEntered.count != 0L) delay(1)
            }

            val teardownJob = async(Dispatchers.IO) { teardown(client) }
            withTimeout(2_000) {
                while (!client.hasQueuedLifecycleTransitionForTest()) delay(1)
            }

            releaseInstall.countDown()
            assertTrue(install.await())
            teardownJob.await()

            assertEquals(ConnectionState.DISCONNECTED, client.state.value)
            assertNull(client.currentSessionForTest(), "teardown must win after the atomic install transition")
        } finally {
            releaseInstall.countDown()
            client.close()
        }
    }

    @Test
    fun `a batch stranded by a requeue is replayed exactly once after reconnect`() = runBlocking {
        val client = StreamlineClient(fastReconnectConfig())
        try {
            client.connect()
            assertEquals(ConnectionState.CONNECTED, client.state.value)

            // Force a disconnect and, while the client is between sessions,
            // seed the batch queue directly -- this is the exact state a
            // flushBatch() requeue (missing session, or a send failing
            // partway through) leaves behind.
            server.closeCurrentConnection()
            awaitCondition { client.state.value == ConnectionState.RECONNECTING }
            client.seedPendingBatch(
                listOf(
                    StreamlineMessage(topic = "orders", value = "stranded-1"),
                    StreamlineMessage(topic = "orders", value = "stranded-2"),
                ),
            )

            awaitCondition { client.state.value == ConnectionState.CONNECTED }
            awaitCondition { server.receivedFrames.any { "stranded-2" in it } }

            assertTrue(client.pendingBatch().isEmpty(), "the replayed batch must not remain stranded")
            assertEquals(
                1,
                server.receivedFrames.count { "stranded-1" in it },
                "a replayed message must be sent exactly once, never duplicated",
            )
            assertEquals(
                1,
                server.receivedFrames.count { "stranded-2" in it },
                "a replayed message must be sent exactly once, never duplicated",
            )
        } finally {
            client.disconnect()
            client.close()
        }
    }

    @Test
    fun `produceBatch preserves an explicit partition over the wire`() = runBlocking {
        val client = StreamlineClient(fastReconnectConfig())
        try {
            client.connect()
            assertEquals(ConnectionState.CONNECTED, client.state.value)

            client.produceBatch(listOf(StreamlineMessage(topic = "orders", value = "routed", partition = 3)))

            awaitCondition { server.receivedFrames.any { "\"value\":\"routed\"" in it } }
            val frame = server.receivedFrames.single { "\"value\":\"routed\"" in it }
            assertTrue("\"partition\":3" in frame, "an explicit partition must never be silently dropped: $frame")
        } finally {
            client.disconnect()
            client.close()
        }
    }
}
