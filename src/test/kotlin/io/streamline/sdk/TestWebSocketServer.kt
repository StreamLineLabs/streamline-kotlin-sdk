package io.streamline.sdk

import io.ktor.server.application.install
import io.ktor.server.cio.CIO
import io.ktor.server.engine.embeddedServer
import io.ktor.server.routing.routing
import io.ktor.server.websocket.DefaultWebSocketServerSession
import io.ktor.server.websocket.WebSockets
import io.ktor.server.websocket.webSocket
import io.ktor.websocket.CloseReason
import io.ktor.websocket.Frame
import io.ktor.websocket.close
import io.ktor.websocket.readText
import java.net.ServerSocket
import java.util.concurrent.CopyOnWriteArrayList
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

/**
 * Minimal in-process WebSocket server used to exercise [StreamlineClient]'s
 * real connect/reconnect lifecycle against an actual session, rather than a
 * hand-mocked one. Bound to loopback only; no external network involved.
 */
internal class TestWebSocketServer {

    /** All text frames received across every connection, in arrival order. */
    val receivedFrames = CopyOnWriteArrayList<String>()

    @Volatile private var currentSession: DefaultWebSocketServerSession? = null
    private val stateMutex = Mutex()

    val port: Int = findFreePort()
    val wsUrl: String = "ws://127.0.0.1:$port/"

    private val server = embeddedServer(CIO, port = port, host = "127.0.0.1") {
        install(WebSockets)
        routing {
            webSocket("/") {
                currentSession = this
                for (frame in incoming) {
                    if (frame is Frame.Text) {
                        stateMutex.withLock { receivedFrames.add(frame.readText()) }
                    }
                }
            }
        }
    }

    fun start() {
        server.start(wait = false)
    }

    fun stop() {
        server.stop(gracePeriodMillis = 0, timeoutMillis = 1000)
    }

    /**
     * Cleanly closes whatever connection is currently open, from the server
     * side, with a normal WebSocket close handshake (no exception on either
     * side). This is what exercises "normal incoming-loop completion" on the
     * client without ever throwing — the closest real-world equivalent of a
     * server or load balancer ending an idle/rotated connection gracefully.
     *
     * No-op if there is currently no open connection.
     */
    fun closeCurrentConnection() {
        val session = currentSession ?: return
        currentSession = null
        runBlocking { session.close(CloseReason(CloseReason.Codes.NORMAL, "test-initiated close")) }
    }

    /** Number of frames received so far whose decoded JSON action equals [action]. */
    fun countAction(action: String): Int = receivedFrames.count { """"action":"$action"""" in it }

    private fun findFreePort(): Int = ServerSocket(0).use { it.localPort }
}
