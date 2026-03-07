package io.streamline.sdk

import io.ktor.client.*
import io.ktor.client.engine.cio.*
import io.ktor.client.plugins.websocket.*
import io.ktor.client.request.*
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
import java.io.FileInputStream
import java.security.KeyStore
import javax.net.ssl.KeyManagerFactory
import javax.net.ssl.TrustManagerFactory

/** Builds the default Ktor HTTP client with TLS if configured. */
private fun buildDefaultHttpClient(config: StreamlineConfiguration): HttpClient {
    return HttpClient(CIO) {
        install(WebSockets)

        val tls = config.tls
        if (tls != null && tls.enabled) {
            engine {
                https {
                    if (tls.insecureSkipVerify) {
                        trustManager = io.ktor.network.tls.TLSConfigBuilder().apply {
                            // Development only — accept all certificates
                        }.build().trustManager
                    }

                    tls.trustStorePath?.let { path ->
                        val ks = KeyStore.getInstance(KeyStore.getDefaultType())
                        FileInputStream(path).use { fis ->
                            ks.load(fis, tls.trustStorePassword?.toCharArray())
                        }
                        val tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm())
                        tmf.init(ks)
                        trustManager = tmf.trustManagers.first() as javax.net.ssl.X509TrustManager
                    }

                    tls.keyStorePath?.let { path ->
                        val ks = KeyStore.getInstance(KeyStore.getDefaultType())
                        FileInputStream(path).use { fis ->
                            ks.load(fis, tls.keyStorePassword?.toCharArray())
                        }
                        val kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm())
                        kmf.init(ks, tls.keyStorePassword?.toCharArray())
                    }
                }
            }
        }
    }
}

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
    private val httpClient: HttpClient = buildDefaultHttpClient(configuration),
) {

    private val json = Json { ignoreUnknownKeys = true; encodeDefaults = true }

    // -- Resilience --

    /** Circuit breaker protecting send operations. */
    val circuitBreaker = CircuitBreaker(configuration.circuitBreakerConfig)

    /** Retry policy for transient failures. */
    val retryPolicy = RetryPolicy(configuration.retryPolicyConfig)

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
            val session = httpClient.webSocketSession(configuration.url) {
                // Apply bearer token or SASL credentials as auth header
                val token = configuration.authToken
                val sasl = configuration.sasl
                when {
                    token != null -> header("Authorization", "Bearer $token")
                    sasl != null -> {
                        val credentials = java.util.Base64.getEncoder()
                            .encodeToString("${sasl.username}:${sasl.password}".toByteArray())
                        header("Authorization", "Basic $credentials")
                        header("X-Streamline-SASL-Mechanism", sasl.mechanism.name)
                    }
                }
            }
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

    // -- Admin --

    /**
     * Creates an [AdminClient] that inherits auth configuration from this client.
     *
     * The admin client uses HTTP (default port 9094) while the streaming client
     * uses WebSocket (default port 9092). Pass the HTTP base URL or let it
     * default to deriving from the WebSocket URL.
     *
     * ```kotlin
     * val client = StreamlineClient(config)
     * val admin = client.admin("http://localhost:9094")
     * val topics = admin.listTopics()
     * ```
     */
    fun admin(httpBaseUrl: String): AdminClient {
        return AdminClient(
            baseUrl = httpBaseUrl,
            authToken = configuration.authToken,
            saslConfig = configuration.sasl,
        )
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

        if (messages.isEmpty()) return
        val session = wsSession ?: return

        if (messages.size == 1) {
            sendWithRetry(messages.first(), session)
        } else {
            // Send as a batch array for efficiency
            retryPolicy.execute {
                circuitBreaker.execute {
                    val batchPayload = json.encodeToString(messages)
                    val wrapper = """{"action":"produce_batch","messages":$batchPayload}"""
                    session.send(Frame.Text(wrapper))
                }
            }
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
        retryPolicy.execute {
            circuitBreaker.execute {
                val payload = buildPayload(message)
                session.send(Frame.Text(payload))
            }
        }
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
        circuitBreaker.execute {
            val command = """{"action":"subscribe","topic":"$topic"}"""
            session.send(Frame.Text(command))
        }
    }

    /** Remove the subscription for the given topic. */
    suspend fun unsubscribe(topic: String) {
        subscriptionsMutex.withLock {
            subscriptions.remove(topic)
        }

        val session = wsSession ?: return
        circuitBreaker.execute {
            val command = """{"action":"unsubscribe","topic":"$topic"}"""
            session.send(Frame.Text(command))
        }
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
