package io.streamline.sdk

import io.ktor.client.*
import io.ktor.client.engine.cio.*
import io.ktor.client.plugins.websocket.*
import io.ktor.websocket.*
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.callbackFlow
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json

/** Closure invoked when a message arrives on a subscribed topic. */
typealias MessageHandler = suspend (StreamlineMessage) -> Unit

/**
 * Primary entry-point for interacting with a Streamline server over WebSocket.
 *
 * The client supports automatic reconnection with exponential backoff and an
 * offline message queue that buffers produce calls while disconnected.
 *
 * ```kotlin
 * val client = StreamlineClient(StreamlineConfiguration(url = "ws://localhost:9092"))
 * client.connect()
 * client.produce("events", key = "user-1", value = "{\"action\":\"click\"}")
 * client.subscribe("events") { msg -> println(msg.value) }
 * client.disconnect()
 * ```
 */
class StreamlineClient(
    private val configuration: StreamlineConfiguration,
    private val httpClient: HttpClient = HttpClient(CIO) { install(WebSockets) },
) {

    private val json = Json { ignoreUnknownKeys = true; encodeDefaults = true }

    // -- Connection state --

    private val _state = MutableStateFlow(ConnectionState.DISCONNECTED)

    /** Observable connection state. */
    val state: StateFlow<ConnectionState> = _state.asStateFlow()

    private var wsSession: DefaultClientWebSocketSession? = null
    private var receiveJob: Job? = null
    private var reconnectJob: Job? = null

    // -- Subscriptions --

    private val subscriptionsMutex = Mutex()
    private val subscriptions = mutableMapOf<String, MessageHandler>()

    // -- Offline queue --

    private val offlineQueue = Channel<StreamlineMessage>(capacity = 1000)
    private val maxOfflineQueueSize = 1000
    private var offlineQueueCount = 0
    private val queueMutex = Mutex()

    private var retryCount = 0

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)

    // -- Producer batching --

    var producerConfig: ProducerConfig = ProducerConfig()
    private val batchMutex = Mutex()
    private val batchQueue = mutableListOf<StreamlineMessage>()
    private var batchFlushJob: Job? = null

    // -- Connection lifecycle --

    /** Open a WebSocket connection to the configured server URL. */
    suspend fun connect() {
        if (_state.value == ConnectionState.CONNECTED || _state.value == ConnectionState.CONNECTING) return

        val isReconnecting = _state.value == ConnectionState.RECONNECTING
        if (!isReconnecting) _state.value = ConnectionState.CONNECTING

        try {
            val session = httpClient.webSocketSession(configuration.url) {}
            wsSession = session
            _state.value = ConnectionState.CONNECTED
            retryCount = 0
            startReceiving(session)
            drainOfflineQueue()
        } catch (e: Exception) {
            handleDisconnection(e)
        }
    }

    /** Gracefully close the connection. */
    suspend fun disconnect() {
        reconnectJob?.cancel()
        reconnectJob = null
        receiveJob?.cancel()
        receiveJob = null

        wsSession?.close(CloseReason(CloseReason.Codes.NORMAL, "Client disconnect"))
        wsSession = null
        _state.value = ConnectionState.DISCONNECTED
    }

    /** Release all resources. Call when the client is no longer needed. */
    fun close() {
        scope.cancel()
        httpClient.close()
    }

    // -- Produce --

    /**
     * Send a message to the given topic. Messages are accumulated into batches
     * and flushed when the batch reaches [ProducerConfig.batchSize] bytes or
     * after [ProducerConfig.lingerMs] milliseconds, whichever comes first.
     * If disconnected, the message is placed in the offline queue.
     */
    suspend fun produce(topic: String, key: String? = null, value: String) {
        val message = StreamlineMessage(topic = topic, key = key, value = value)

        val session = wsSession
        if (_state.value != ConnectionState.CONNECTED || session == null) {
            enqueueOffline(message)
            return
        }

        var shouldFlush = false
        batchMutex.withLock {
            batchQueue.add(message)
            val totalBytes = batchQueue.sumOf { it.value.toByteArray().size }
            shouldFlush = totalBytes >= producerConfig.batchSize
        }

        if (shouldFlush) {
            flushBatch()
        } else {
            scheduleLingerFlush()
        }
    }

    /** Flush all pending batched messages immediately. */
    suspend fun flushBatch() {
        val messages: List<StreamlineMessage>
        batchMutex.withLock {
            batchFlushJob?.cancel()
            batchFlushJob = null
            messages = batchQueue.toList()
            batchQueue.clear()
        }

        val session = wsSession ?: return
        for (message in messages) {
            sendWithRetry(message, session)
        }
    }

    private fun scheduleLingerFlush() {
        scope.launch {
            batchMutex.withLock {
                if (batchFlushJob != null) return@launch
                val lingerMs = producerConfig.lingerMs.coerceAtLeast(1)
                batchFlushJob = scope.launch {
                    delay(lingerMs)
                    flushBatch()
                }
            }
        }
    }

    private suspend fun sendWithRetry(message: StreamlineMessage, session: DefaultClientWebSocketSession) {
        val maxRetries = producerConfig.retries
        val backoffMs = producerConfig.retryBackoffMs
        var lastException: Exception? = null

        for (attempt in 0..maxRetries) {
            try {
                val payload = buildPayload(message)
                session.send(Frame.Text(payload))
                return
            } catch (e: Exception) {
                lastException = e
                if (attempt < maxRetries) {
                    val delayMs = backoffMs * (1L shl attempt.coerceAtMost(10))
                    delay(delayMs)
                }
            }
        }

        throw ConnectionFailedException("Send failed after $maxRetries retries", lastException)
    }

    private fun buildPayload(message: StreamlineMessage): String {
        val compression = producerConfig.compression
        return if (compression != CompressionType.NONE) {
            // Include compression metadata for server-side handling
            """{"topic":"${message.topic}","key":${message.key?.let { "\"$it\"" } ?: "null"},"value":"${message.value}","compression":"${compression.name.lowercase()}"}"""
        } else {
            json.encodeToString(message)
        }
    }

    // -- Subscribe / Unsubscribe --

    /** Register a handler for messages on the given topic. */
    suspend fun subscribe(topic: String, handler: MessageHandler) {
        subscriptionsMutex.withLock {
            subscriptions[topic] = handler
        }

        val session = wsSession ?: return
        val command = """{"action":"subscribe","topic":"$topic"}"""
        session.send(Frame.Text(command))
    }

    /** Remove the subscription for the given topic. */
    suspend fun unsubscribe(topic: String) {
        subscriptionsMutex.withLock {
            subscriptions.remove(topic)
        }

        val session = wsSession ?: return
        val command = """{"action":"unsubscribe","topic":"$topic"}"""
        session.send(Frame.Text(command))
    }

    // -- Flow-based Consumption --

    /**
     * Returns a [Flow] of messages for the given topic. The flow subscribes
     * on collection and unsubscribes when the collector is cancelled.
     *
     * ```kotlin
     * client.messages("events").collect { msg ->
     *     println("Got: ${msg.value}")
     * }
     * ```
     */
    fun messages(topic: String): Flow<StreamlineMessage> = callbackFlow {
        subscribe(topic) { message ->
            trySend(message)
        }
        awaitClose {
            scope.launch { unsubscribe(topic) }
        }
    }

    // -- Internals --

    private fun startReceiving(session: DefaultClientWebSocketSession) {
        receiveJob = scope.launch {
            try {
                for (frame in session.incoming) {
                    when (frame) {
                        is Frame.Text -> handleIncoming(frame.readText())
                        is Frame.Binary -> handleIncoming(frame.readBytes().decodeToString())
                        else -> { /* ignore */ }
                    }
                }
            } catch (_: CancellationException) {
                // Normal shutdown
            } catch (e: Exception) {
                handleDisconnection(e)
            }
        }
    }

    private suspend fun handleIncoming(text: String) {
        val message = try {
            json.decodeFromString<StreamlineMessage>(text)
        } catch (_: Exception) {
            return
        }

        val handler = subscriptionsMutex.withLock { subscriptions[message.topic] }
        handler?.invoke(message)
    }

    // -- Reconnection --

    private fun handleDisconnection(cause: Exception? = null) {
        wsSession = null
        receiveJob?.cancel()
        receiveJob = null

        if (!configuration.autoReconnect || retryCount >= configuration.maxRetries) {
            _state.value = ConnectionState.DISCONNECTED
            return
        }

        _state.value = ConnectionState.RECONNECTING
        retryCount++
        val attempt = retryCount

        val backoffMs = minOf(
            configuration.initialBackoffMs * (1L shl (attempt - 1).coerceAtMost(30)),
            configuration.maxBackoffMs,
        )

        reconnectJob = scope.launch {
            delay(backoffMs)
            if (isActive) connect()
        }
    }

    // -- Offline Queue --

    private suspend fun enqueueOffline(message: StreamlineMessage) {
        queueMutex.withLock {
            if (offlineQueueCount >= maxOfflineQueueSize) throw OfflineQueueFullException()
            offlineQueue.send(message)
            offlineQueueCount++
        }
    }

    private fun drainOfflineQueue() {
        scope.launch {
            while (true) {
                val message = offlineQueue.tryReceive().getOrNull() ?: break
                queueMutex.withLock { offlineQueueCount-- }
                try {
                    produce(topic = message.topic, key = message.key, value = message.value)
                } catch (_: Exception) {
                    // Best-effort delivery
                }
            }
        }
    }
}


/**
 * TLS configuration for secure broker connections.
 *
 * @property caCertPath Path to CA certificate file
 * @property clientCertPath Path to client certificate for mutual TLS
 * @property clientKeyPath Path to client private key
 * @property verifyHostname Whether to verify server hostname (default: true)
 */
data class TlsConfig(
    val caCertPath: String? = null,
    val clientCertPath: String? = null,
    val clientKeyPath: String? = null,
    val verifyHostname: Boolean = true,
)
