package io.streamline.sdk

import io.ktor.client.*
import io.ktor.client.engine.cio.*
import io.ktor.client.plugins.websocket.*
import io.ktor.client.request.*
import io.ktor.websocket.*
import java.util.UUID
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.locks.ReentrantLock
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
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive

/** Internal response model for offset queries. */
@Serializable
internal data class OffsetResponse(val offset: Long? = null)

@Serializable
internal data class ProduceCommand(
    val topic: String,
    val key: String?,
    val value: String,
    val acks: String,
    val compression: String? = null,
    val idempotent: Boolean? = null,
    val sequence: Long? = null,
    val headers: Map<String, String>? = null,
    // An explicit StreamlineMessage.partition must never be silently dropped:
    // it is forwarded as-is so the server can honor an explicit routing
    // decision instead of falling back to its default partitioning.
    val partition: Int? = null,
)

/**
 * Outbound WebSocket control-command payloads.
 *
 * These mirror the previous hand-built JSON strings field-for-field (action
 * names and `@SerialName` overrides preserve the exact wire format) but are
 * built through `kotlinx.serialization` instead of raw string interpolation
 * so that arbitrary topic/group/key content can never break out of its JSON
 * string context. `action` intentionally has no default value so it is never
 * dropped by `outboundJson`'s `encodeDefaults = false`.
 */
@Serializable
internal data class SubscribeCommand(
    val action: String,
    val topic: String,
    @SerialName("auto_offset_reset") val autoOffsetReset: String,
    @SerialName("group_id") val groupId: String? = null,
    @SerialName("session_timeout_ms") val sessionTimeoutMs: Long? = null,
    @SerialName("heartbeat_interval_ms") val heartbeatIntervalMs: Long? = null,
)

@Serializable
internal data class TopicActionCommand(
    val action: String,
    val topic: String,
)

@Serializable
internal data class SeekCommand(
    val action: String,
    val topic: String,
    val partition: Int,
    val offset: Long,
)

@Serializable
internal data class TopicPartitionQuery(
    val action: String,
    val topic: String,
    val partition: Int,
)

@Serializable
internal data class OffsetEntry(
    val topicPartition: String,
    val offset: Long,
)

@Serializable
internal data class CommitOffsetsCommand(
    val action: String,
    val offsets: List<OffsetEntry>,
)

@Serializable
internal data class HeartbeatCommand(
    val action: String,
    @SerialName("group_id") val groupId: String,
    @SerialName("session_timeout_ms") val sessionTimeoutMs: Long,
)

internal data class PendingControlResponse(
    val requestId: String,
    val response: CompletableDeferred<String>,
)

private data class HandlerDelivery(
    val handler: MessageHandler,
    val message: StreamlineMessage,
)

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
    private val auth: AuthConfig? = null,
    private val httpClient: HttpClient = HttpClient(CIO) {
        engine { configureTls(configuration.tls) }
        install(WebSockets)
    },
    private val schemaRegistry: SchemaRegistryClient? = null,
) {

    private val json = Json { ignoreUnknownKeys = true; encodeDefaults = true }
    private val outboundJson = Json { encodeDefaults = false; explicitNulls = true }

    // -- Connection state --

    private val _state = MutableStateFlow(ConnectionState.DISCONNECTED)

    /** Observable connection state. */
    val state: StateFlow<ConnectionState> = _state.asStateFlow()

    private var wsSession: DefaultClientWebSocketSession? = null
    private var receiveJob: Job? = null
    private var reconnectJob: Job? = null
    private val controlRequestMutex = Mutex()
    private val pendingControlResponse = AtomicReference<PendingControlResponse?>(null)
    private val lifecycleMutex = ReentrantLock(true)

    // Monotonically increasing identifier for the current session lifecycle.
    // Bumped on every successful connect and on every explicit disconnect so
    // that a stale receive loop or a stale scheduled reconnect — one that
    // belongs to a superseded connection attempt — can detect it is stale
    // and refuse to mutate shared connection state (wsSession, state,
    // retryCount, reconnectJob).
    private val connectionGeneration = AtomicLong(0)

    private inline fun <T> lifecycleTransition(block: () -> T): T {
        lifecycleMutex.lock()
        return try {
            block()
        } finally {
            lifecycleMutex.unlock()
        }
    }

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
    private val handlerQueue = Channel<HandlerDelivery>(capacity = 1000)
    private val handlerDispatchJob = scope.launch {
        for (delivery in handlerQueue) {
            try {
                delivery.handler(delivery.message)
            } catch (e: CancellationException) {
                if (!currentCoroutineContext().isActive) {
                    throw e
                }
            } catch (_: Exception) {
                // A handler failure must not stop delivery to other subscriptions.
            }
        }
    }

    // -- Producer batching --

    var producerConfig: ProducerConfig = ProducerConfig()
        set(value) {
            // Fail closed at assignment time: an acknowledgment/idempotence
            // contract this transport cannot honor must never be accepted,
            // let alone discovered lazily on the next produce() call.
            value.validate()
            field = value
        }
    var consumerConfig: ConsumerConfig = ConsumerConfig()
    private val batchMutex = Mutex()
    private val batchQueue = mutableListOf<StreamlineMessage>()
    private var batchFlushJob: Job? = null
    private var autoCommitJob: Job? = null
    private var heartbeatJob: Job? = null

    // Track consumed offsets per topic-partition for auto-commit
    private val consumedOffsets = mutableMapOf<String, Long>()
    private val offsetsMutex = Mutex()

    // Poll buffer for poll-based consumption
    private val pollBuffer = Channel<StreamlineMessage>(capacity = 10_000)

    // Client-side metrics
    private val _metrics = ClientMetrics()

    /** Exposes read-only client-side metrics. */
    val metrics: ClientMetricsSnapshot get() = _metrics.snapshot()

    // -- Connection lifecycle --

    /** Open a WebSocket connection to the configured server URL. */
    suspend fun connect() {
        // Reserve this attempt's generation *before* the handshake begins,
        // since the handshake below suspends on a network round-trip. This
        // lets a concurrent disconnect()/close()/newer connect() — each of
        // which bumps the generation — be detected the instant the
        // handshake finishes. A handshake that resolves after losing that
        // race is fenced off by installIfCurrent(): closed immediately and
        // never installed as the active session or used to mutate shared
        // connection state.
        val myGeneration =
            lifecycleTransition {
                if (_state.value == ConnectionState.CONNECTED || _state.value == ConnectionState.CONNECTING) {
                    null
                } else {
                    val isReconnecting = _state.value == ConnectionState.RECONNECTING
                    if (!isReconnecting) _state.value = ConnectionState.CONNECTING
                    connectionGeneration.incrementAndGet()
                }
            } ?: return

        try {
            val authHdrs = authHeaders(auth)
            val session = httpClient.webSocketSession(configuration.url) {
                // Apply auth headers to the WebSocket upgrade request
                for ((key, value) in authHdrs) {
                    headers.append(key, value)
                }
                // Legacy authToken support
                if (authHdrs.isEmpty() && configuration.authToken != null) {
                    headers.append("Authorization", bearerAuthorization(configuration.authToken))
                }
            }

            if (!installIfCurrent(myGeneration, session)) return

            if (!startReceiving(session, myGeneration)) return
            replaySubscriptions(session)
            drainOfflineQueue()
            drainPendingBatch()
            startAutoCommit()
            startHeartbeat()
        } catch (e: ConfigurationException) {
            transitionToDisconnectedIfCurrent(myGeneration)
            throw e
        } catch (e: AuthenticationFailedException) {
            transitionToDisconnectedIfCurrent(myGeneration)
            throw e
        } catch (e: Exception) {
            handleDisconnection(e, myGeneration)
        }
    }

    /**
     * Installs [session] as the active session for [myGeneration] if that
     * generation is still current, or fences it off if a concurrent
     * disconnect()/close()/newer connect() already advanced the generation
     * while the handshake that produced [session] was in flight.
     *
     * A fenced-off session is closed immediately and never touches
     * [wsSession], [state], or [retryCount] — a handshake that completes
     * late must never resurrect a connection that was explicitly torn down
     * or superseded while it was still pending.
     *
     * @return `true` if [session] was installed as the active session, `false` if fenced off.
     */
    internal suspend fun installIfCurrent(
        myGeneration: Long,
        session: DefaultClientWebSocketSession,
        beforeInstallForTest: (() -> Unit)? = null,
    ): Boolean {
        val installed =
            lifecycleTransition {
                if (myGeneration != connectionGeneration.get()) {
                    false
                } else {
                    beforeInstallForTest?.invoke()
                    wsSession = session
                    _state.value = ConnectionState.CONNECTED
                    retryCount = 0
                    true
                }
            }
        if (!installed) {
            runCatching { session.close(CloseReason(CloseReason.Codes.NORMAL, "Superseded connection attempt")) }
        }
        return installed
    }

    private fun transitionToDisconnectedIfCurrent(generation: Long) {
        lifecycleTransition {
            if (generation == connectionGeneration.get()) {
                _state.value = ConnectionState.DISCONNECTED
            }
        }
    }

    /**
     * Opens a raw WebSocket session against the configured server without
     * installing it. Exposed only so lifecycle tests can exercise a
     * handshake that resolves independently of (and later than) the
     * client's own [connect] call, to deterministically reproduce a late
     * handshake race against [installIfCurrent].
     */
    internal suspend fun openRawSessionForTest(): DefaultClientWebSocketSession {
        val authHdrs = authHeaders(auth)
        return httpClient.webSocketSession(configuration.url) {
            for ((key, value) in authHdrs) {
                headers.append(key, value)
            }
            if (authHdrs.isEmpty() && configuration.authToken != null) {
                headers.append("Authorization", bearerAuthorization(configuration.authToken))
            }
        }
    }

    /** Exposes the currently installed session, if any; used by lifecycle tests. */
    internal fun currentSessionForTest(): DefaultClientWebSocketSession? =
        lifecycleTransition { wsSession }

    /** Gracefully close the connection. */
    suspend fun disconnect() {
        // Final commit before disconnect
        if (consumerConfig.autoCommit) {
            try { flushAutoCommit() } catch (_: Exception) { /* best-effort */ }
        }

        val sessionToClose =
            lifecycleTransition {
                // Invalidate and detach the current lifecycle atomically with
                // session/state mutation, so installIfCurrent() cannot pass its
                // generation check and resurrect a session between these writes.
                connectionGeneration.incrementAndGet()
                heartbeatJob?.cancel()
                heartbeatJob = null
                autoCommitJob?.cancel()
                autoCommitJob = null
                reconnectJob?.cancel()
                reconnectJob = null
                receiveJob?.cancel()
                receiveJob = null
                val current = wsSession
                wsSession = null
                _state.value = ConnectionState.DISCONNECTED
                current
            }
        failPendingControlResponse(NotConnectedException())
        sessionToClose?.close(CloseReason(CloseReason.Codes.NORMAL, "Client disconnect"))
    }

    /** Release all resources. Call when the client is no longer needed. */
    fun close() {
        val sessionToCancel =
            lifecycleTransition {
                // Fence off pending handshakes and detach the installed session
                // in one transition. Session cancellation happens after the lock
                // is released so callbacks cannot run while lifecycle state is
                // locked.
                connectionGeneration.incrementAndGet()
                heartbeatJob?.cancel()
                heartbeatJob = null
                autoCommitJob?.cancel()
                autoCommitJob = null
                reconnectJob?.cancel()
                reconnectJob = null
                receiveJob?.cancel()
                receiveJob = null
                val current = wsSession
                wsSession = null
                _state.value = ConnectionState.DISCONNECTED
                current
            }
        failPendingControlResponse(NotConnectedException())
        scope.cancel()
        sessionToCancel?.cancel(CancellationException("Client closed"))
        httpClient.close()
    }

    // -- Produce --

    // Sequence number for idempotent production
    private var producerSequence: Long = 0L
    private val sequenceMutex = Mutex()

    /**
     * Send a message to the given topic. Messages are accumulated into batches
     * and flushed when the batch reaches [ProducerConfig.batchSize] bytes or
     * after [ProducerConfig.lingerMs] milliseconds, whichever comes first.
     * If disconnected, the message is placed in the offline queue.
     */
    suspend fun produce(topic: String, key: String? = null, value: String) {
        TopicNameValidator.validate(topic)
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

    /**
     * Flush all pending batched messages immediately.
     *
     * If the connection is currently missing, or a send fails partway
     * through the batch, every message that was not confirmed sent is put
     * back at the front of the batch queue (preserving original order) so a
     * later flush retries them exactly once each — no message is dropped,
     * and no already-sent message is resent.
     */
    suspend fun flushBatch() {
        val messages: List<StreamlineMessage>
        batchMutex.withLock {
            batchFlushJob?.cancel()
            batchFlushJob = null
            messages = batchQueue.toList()
            batchQueue.clear()
        }
        if (messages.isEmpty()) return

        val session = wsSession
        if (session == null) {
            requeueBatch(messages)
            return
        }

        sendBatchOrRequeue(messages) { message -> sendWithRetry(message, session) }
    }

    /**
     * Sends [messages] one at a time via [sender]. On the first failure,
     * that message and every message after it (which were never attempted)
     * are requeued at the front of the batch queue via [requeueBatch] and
     * the triggering exception is rethrown so a caller awaiting an explicit
     * [flushBatch] observes the failure. Messages that already sent
     * successfully are never requeued or resent.
     */
    internal suspend fun sendBatchOrRequeue(
        messages: List<StreamlineMessage>,
        sender: suspend (StreamlineMessage) -> Unit,
    ) {
        var failure: Exception? = null
        val unsent = mutableListOf<StreamlineMessage>()
        for (message in messages) {
            if (failure != null) {
                unsent.add(message)
                continue
            }
            try {
                sender(message)
            } catch (e: Exception) {
                failure = e
                unsent.add(message)
            }
        }
        if (unsent.isNotEmpty()) {
            requeueBatch(unsent)
        }
        failure?.let { throw it }
    }

    /** Puts [messages] back at the front of the batch queue, preserving order. */
    private suspend fun requeueBatch(messages: List<StreamlineMessage>) {
        batchMutex.withLock { batchQueue.addAll(0, messages) }
    }

    /** Exposes pending (unflushed) batch contents; used by lifecycle tests. */
    internal suspend fun pendingBatch(): List<StreamlineMessage> = batchMutex.withLock { batchQueue.toList() }

    /** Seeds the batch queue directly; used by lifecycle tests to simulate a disconnect race. */
    internal suspend fun seedPendingBatch(messages: List<StreamlineMessage>) {
        batchMutex.withLock { batchQueue.addAll(messages) }
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
        val startMs = System.currentTimeMillis()

        for (attempt in 0..maxRetries) {
            try {
                val payload = buildPayload(message)
                session.send(Frame.Text(payload))
                _metrics.recordProduce(
                    bytes = message.value.toByteArray().size.toLong(),
                    latencyMs = System.currentTimeMillis() - startMs,
                )
                return
            } catch (e: Exception) {
                lastException = e
                _metrics.recordProduceError()
                if (attempt < maxRetries) {
                    val delayMs = backoffMs * (1L shl attempt.coerceAtMost(10))
                    delay(delayMs)
                }
            }
        }

        throw ConnectionFailedException("Send failed after $maxRetries retries", lastException)
    }

    internal suspend fun buildPayload(message: StreamlineMessage): String {
        // Fail closed at the send boundary too: producerConfig can only be
        // assigned through a validating setter, but this is the funnel every
        // produce()/flushBatch()/produceBatch() send goes through, so it is
        // asserted here as well rather than trusted transitively.
        producerConfig.validate()
        validatePartition(message.partition)

        val compression = producerConfig.compression
        val acks = producerConfig.acks
        val idempotent = producerConfig.idempotent
        val seq = if (idempotent) {
            sequenceMutex.withLock { producerSequence++ }
        } else {
            null
        }
        val acksStr = when (acks) {
            Acks.NONE -> "0"
            Acks.ONE -> "1"
            Acks.ALL -> "all"
        }

        return outboundJson.encodeToString(
            ProduceCommand(
                topic = message.topic,
                key = message.key,
                value = message.value,
                acks = acksStr,
                compression = compression
                    .takeUnless { it == CompressionType.NONE }
                    ?.name
                    ?.lowercase(),
                idempotent = true.takeIf { idempotent },
                sequence = seq,
                headers = message.headers.takeIf { it.isNotEmpty() },
                // Preserve an explicit partition assignment as-is; never
                // silently drop it in favor of the server's default
                // partitioning.
                partition = message.partition,
            )
        )
    }

    /**
     * Validates an explicit partition assignment before it is sent, so an
     * invalid value is rejected up front instead of being silently dropped
     * or forwarded unchecked.
     */
    private fun validatePartition(partition: Int?) {
        require(partition == null || partition >= 0) {
            "StreamlineMessage.partition must be >= 0 when explicitly set, was $partition"
        }
    }

    // -- Schema-Aware Production --

    /**
     * Run the historical schema-registry compatibility check and then produce
     * the original string unchanged.
     *
     * This is a validation-only compatibility API: [value] is submitted to the
     * registry compatibility endpoint for the topic's `topic-value` subject.
     * The SDK does not validate encoded record data, prepend a schema id, or
     * implement Confluent wire framing. Applications that produce records must
     * validate and frame them with their chosen schema serializer before
     * calling [produce].
     *
     * @throws SchemaRegistryException if schema validation or lookup fails.
     * @throws IllegalStateException if no [SchemaRegistryClient] was provided.
     */
    suspend fun produceWithSchema(
        topic: String,
        key: String? = null,
        value: String,
        schemaFormat: SchemaFormat = SchemaFormat.AVRO,
    ) {
        val registry = schemaRegistry
            ?: throw IllegalStateException("SchemaRegistryClient not configured. Pass it to the StreamlineClient constructor.")

        val subject = "$topic-value"
        // Compatibility validation only; no record encoding or schema framing.
        val compatible = registry.checkCompatibility(subject, value, schemaFormat)
        if (!compatible) {
            throw SchemaRegistryException(
                "Value is not compatible with the latest schema for subject '$subject'"
            )
        }

        produce(topic, key, value)
    }

    // -- Subscribe / Unsubscribe --

    /** Register a handler for messages on the given topic. */
    suspend fun subscribe(topic: String, handler: MessageHandler) {
        TopicNameValidator.validate(topic)
        subscriptionsMutex.withLock {
            subscriptions[topic] = handler
        }

        val session = wsSession ?: return
        val groupId = consumerConfig.groupId
        session.send(Frame.Text(buildSubscribeCommand(topic)))

        // Apply offset reset strategy for non-grouped consumers
        if (groupId == null) {
            when (consumerConfig.autoOffsetReset) {
                OffsetReset.EARLIEST -> seekToBeginning(topic)
                OffsetReset.LATEST -> { /* server default is latest */ }
                OffsetReset.NONE -> { /* no reset — fail if no committed offset */ }
            }
        }
    }

    private fun buildSubscribeCommand(topic: String): String {
        val groupId = consumerConfig.groupId
        val resetStrategy = consumerConfig.autoOffsetReset.name.lowercase()
        return outboundJson.encodeToString(
            SubscribeCommand(
                action = "subscribe",
                topic = topic,
                autoOffsetReset = resetStrategy,
                groupId = groupId,
                sessionTimeoutMs = groupId?.let { consumerConfig.sessionTimeoutMs },
                heartbeatIntervalMs = groupId?.let { consumerConfig.heartbeatIntervalMs },
            )
        )
    }

    /**
     * Re-sends a `subscribe` command for every currently active subscription
     * on a freshly (re)established session. The server has no memory of a
     * dropped connection's subscriptions, so without this a reconnect would
     * silently stop delivering messages for topics the caller is still
     * subscribed to — even though [subscriptions] and [state] look healthy.
     *
     * This intentionally does not re-seek offsets: only the initial
     * [subscribe] call applies [ConsumerConfig.autoOffsetReset], so a
     * reconnect resumes from the broker's view of the consumer's position
     * instead of rewinding progress on every drop.
     */
    private suspend fun replaySubscriptions(session: DefaultClientWebSocketSession) {
        val topics = subscriptionsMutex.withLock { subscriptions.keys.toList() }
        for (topic in topics) {
            try {
                session.send(Frame.Text(buildSubscribeCommand(topic)))
            } catch (_: Exception) {
                // Best-effort: a send failure here means the session is
                // already broken, which the receive loop will observe and
                // turn into another disconnect/reconnect/replay cycle.
            }
        }
    }

    /** Remove the subscription for the given topic. */
    suspend fun unsubscribe(topic: String) {
        TopicNameValidator.validate(topic)
        subscriptionsMutex.withLock {
            subscriptions.remove(topic)
        }

        val session = wsSession ?: return
        val command = outboundJson.encodeToString(TopicActionCommand(action = "unsubscribe", topic = topic))
        session.send(Frame.Text(command))
    }

    // -- Consumer Offset Management --

    /**
     * Commit consumer offsets for the given topic-partition pairs.
     * Waits for server acknowledgement within the configured timeout.
     *
     * @param offsets Map of "topic:partition" to offset value.
     * @throws StreamlineTimeoutException if the server does not respond in time.
     */
    suspend fun commitOffsets(offsets: Map<String, Long>) {
        val session = wsSession
            ?: throw NotConnectedException()

        val command = buildCommitOffsetsCommand(offsets)
        val response = awaitControlResponse(
            session = session,
            command = command,
            timeoutMessage = "Offset commit acknowledgement timed out",
        )
        if (response.contains("\"error\"")) {
            throw StreamlineException(
                "Offset commit rejected by server: $response",
                errorCode = ErrorCode.INTERNAL,
            )
        }
    }

    /**
     * Builds the `commit_offsets` wire payload via `kotlinx.serialization` so
     * that arbitrary "topic:partition" key content is always properly
     * JSON-escaped and can never break out of its string context.
     */
    internal fun buildCommitOffsetsCommand(offsets: Map<String, Long>): String {
        val entries = offsets.map { (key, offset) -> OffsetEntry(topicPartition = key, offset = offset) }
        return outboundJson.encodeToString(CommitOffsetsCommand(action = "commit_offsets", offsets = entries))
    }

    private suspend fun awaitControlResponse(
        session: DefaultClientWebSocketSession,
        command: String,
        timeoutMessage: String,
    ): String = controlRequestMutex.withLock {
        val pending = registerControlResponse()
        try {
            session.send(Frame.Text(addRequestId(command, pending.requestId)))
            withTimeout(configuration.timeoutMs) {
                pending.response.await()
            }
        } catch (e: kotlinx.coroutines.TimeoutCancellationException) {
            throw StreamlineTimeoutException(timeoutMessage)
        } finally {
            pendingControlResponse.compareAndSet(pending, null)
        }
    }

    internal fun registerControlResponse(
        requestId: String = UUID.randomUUID().toString(),
    ): PendingControlResponse {
        val pending = PendingControlResponse(
            requestId = requestId,
            response = CompletableDeferred(),
        )
        if (!pendingControlResponse.compareAndSet(null, pending)) {
            throw ProtocolException("Another control response is already pending")
        }
        return pending
    }

    internal fun addRequestId(command: String, requestId: String): String {
        val commandObject = json.parseToJsonElement(command).jsonObject
        val correlatedCommand = JsonObject(
            commandObject + ("request_id" to JsonPrimitive(requestId))
        )
        return outboundJson.encodeToString(JsonObject.serializer(), correlatedCommand)
    }

    /** Current connection generation, exposed for reconnect/lifecycle tests. */
    internal fun currentGeneration(): Long = lifecycleTransition { connectionGeneration.get() }

    /** Whether a teardown is queued behind an in-progress lifecycle transition. */
    internal fun hasQueuedLifecycleTransitionForTest(): Boolean = lifecycleMutex.hasQueuedThreads()

    private fun failPendingControlResponse(cause: Throwable) {
        pendingControlResponse.getAndSet(null)?.response?.completeExceptionally(cause)
    }

    /**
     * Seek the consumer to a specific offset for a topic partition.
     */
    suspend fun seekToOffset(topic: String, partition: Int, offset: Long) {
        TopicNameValidator.validate(topic)
        val session = wsSession
            ?: throw NotConnectedException()

        val command = outboundJson.encodeToString(
            SeekCommand(action = "seek", topic = topic, partition = partition, offset = offset)
        )
        session.send(Frame.Text(command))
    }

    /** Seek the consumer to the beginning of all partitions for the given topic. */
    suspend fun seekToBeginning(topic: String) {
        TopicNameValidator.validate(topic)
        val session = wsSession
            ?: throw NotConnectedException()

        val command = outboundJson.encodeToString(TopicActionCommand(action = "seek_to_beginning", topic = topic))
        session.send(Frame.Text(command))
    }

    /** Seek the consumer to the end (latest) of all partitions for the given topic. */
    suspend fun seekToEnd(topic: String) {
        TopicNameValidator.validate(topic)
        val session = wsSession
            ?: throw NotConnectedException()

        val command = outboundJson.encodeToString(TopicActionCommand(action = "seek_to_end", topic = topic))
        session.send(Frame.Text(command))
    }

    /**
     * Get the current consumer position (next offset to be read) for a topic partition.
     *
     * @return The current position, or null if not available.
     */
    suspend fun position(topic: String, partition: Int): Long? {
        TopicNameValidator.validate(topic)
        val session = wsSession
            ?: throw NotConnectedException()

        val command = outboundJson.encodeToString(
            TopicPartitionQuery(action = "position", topic = topic, partition = partition)
        )
        val response = awaitControlResponse(
            session = session,
            command = command,
            timeoutMessage = "Position query timed out",
        )
        return try {
            json.decodeFromString<OffsetResponse>(response).offset
        } catch (e: Exception) {
            throw ProtocolException("Invalid position response", e)
        }
    }

    /**
     * Get the last committed offset for a topic partition.
     *
     * @return The committed offset, or null if no offset has been committed.
     */
    suspend fun committed(topic: String, partition: Int): Long? {
        TopicNameValidator.validate(topic)
        val session = wsSession
            ?: throw NotConnectedException()

        val command = outboundJson.encodeToString(
            TopicPartitionQuery(action = "committed", topic = topic, partition = partition)
        )
        val response = awaitControlResponse(
            session = session,
            command = command,
            timeoutMessage = "Committed offset query timed out",
        )
        return try {
            json.decodeFromString<OffsetResponse>(response).offset
        } catch (e: Exception) {
            throw ProtocolException("Invalid committed offset response", e)
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

    private fun startReceiving(session: DefaultClientWebSocketSession, generation: Long): Boolean {
        val job = scope.launch(start = CoroutineStart.LAZY) {
            try {
                for (frame in session.incoming) {
                    when (frame) {
                        is Frame.Text -> dispatchIncoming(frame.readText())
                        is Frame.Binary -> dispatchIncoming(frame.readBytes().decodeToString())
                        else -> { /* ignore */ }
                    }
                }
                // The `incoming` channel completed without throwing. Ktor
                // closes it this way once the WebSocket close handshake
                // finishes (e.g. the server closed the connection cleanly),
                // so a normal loop exit is still a disconnection — not a
                // no-op — whenever this session is still the active one.
                handleDisconnection(cause = null, generation = generation)
            } catch (_: CancellationException) {
                // Normal shutdown: disconnect()/close() cancelled this job.
            } catch (e: Exception) {
                handleDisconnection(e, generation)
            }
        }
        val installed =
            lifecycleTransition {
                if (generation != connectionGeneration.get() || wsSession !== session) {
                    false
                } else {
                    receiveJob = job
                    true
                }
            }
        if (installed) {
            job.start()
        } else {
            job.cancel()
        }
        return installed
    }

    internal suspend fun dispatchIncoming(text: String) {
        val message = try {
            json.decodeFromString<StreamlineMessage>(text)
        } catch (_: Exception) {
            dispatchControlResponse(text)
            return
        }

        val handler = subscriptionsMutex.withLock { subscriptions[message.topic] }
        val acceptedByHandler = if (handler != null) {
            handlerQueue.send(HandlerDelivery(handler, message))
            true
        } else {
            false
        }
        val acceptedByPoll = pollBuffer.trySend(message).isSuccess
        if (!acceptedByHandler && !acceptedByPoll) {
            return
        }

        // Advance offsets only after a delivery path has accepted the message.
        val offset = message.offset
        val partition = message.partition
        if (offset != null && partition != null) {
            val tpKey = "${message.topic}:$partition"
            offsetsMutex.withLock {
                val current = consumedOffsets[tpKey]
                if (current == null || offset > current) {
                    consumedOffsets[tpKey] = offset + 1 // commit the *next* offset
                }
            }
        }

        // Record consume metric
        _metrics.recordConsume(message.value.toByteArray().size.toLong())
    }

    private fun dispatchControlResponse(text: String) {
        val responseObject = try {
            json.parseToJsonElement(text).jsonObject
        } catch (_: Exception) {
            return
        }
        val requestId = responseObject["request_id"]
            ?.jsonPrimitive
            ?.contentOrNull
            ?: return
        val pending = pendingControlResponse.get() ?: return
        if (pending.requestId != requestId) return
        if (pendingControlResponse.compareAndSet(pending, null)) {
            pending.response.complete(text)
        }
    }

    // -- Auto-Commit --

    private fun startAutoCommit() {
        if (!consumerConfig.autoCommit) return

        autoCommitJob?.cancel()
        autoCommitJob = scope.launch {
            while (isActive) {
                delay(consumerConfig.autoCommitIntervalMs)
                try {
                    flushAutoCommit()
                } catch (_: Exception) {
                    // Best-effort; will retry on next interval
                }
            }
        }
    }

    private suspend fun flushAutoCommit() {
        val snapshot = offsetsMutex.withLock {
            if (consumedOffsets.isEmpty()) return
            consumedOffsets.toMap()
        }
        if (snapshot.isNotEmpty()) {
            commitOffsets(snapshot)
        }
    }

    // -- Reconnection --

    private fun handleDisconnection(cause: Exception? = null, generation: Long = connectionGeneration.get()) {
        var handled = false
        lifecycleTransition {
            // A stale receive loop must not mutate the session, state, retry
            // counters, or reconnect job belonging to a newer generation.
            if (generation != connectionGeneration.get()) return@lifecycleTransition

            handled = true
            wsSession = null
            receiveJob?.cancel()
            receiveJob = null

            if (!configuration.autoReconnect || retryCount >= configuration.maxRetries) {
                _state.value = ConnectionState.DISCONNECTED
                return@lifecycleTransition
            }

            _state.value = ConnectionState.RECONNECTING
            retryCount++
            val attempt = retryCount
            val backoffMs =
                minOf(
                    configuration.initialBackoffMs * (1L shl (attempt - 1).coerceAtMost(30)),
                    configuration.maxBackoffMs,
                )

            reconnectJob =
                scope.launch {
                    delay(backoffMs)
                    val isCurrent = lifecycleTransition { generation == connectionGeneration.get() }
                    if (isActive && isCurrent) connect()
                }
        }
        if (handled) {
            failPendingControlResponse(
                ConnectionFailedException("WebSocket disconnected", cause),
            )
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

    /**
     * Replays any batch content stranded by a prior [flushBatch] requeue
     * (missing session, or a send that failed partway through the batch) on
     * a freshly (re)established session.
     *
     * Without this, a batch that lands in [batchQueue] via [requeueBatch]
     * has nothing left to trigger a later flush unless a new [produce] call
     * happens to arrive and re-trip the batch-size/linger thresholds — it
     * would otherwise sit stranded indefinitely across a reconnect that
     * looks healthy ([state] == CONNECTED). [flushBatch] itself already
     * guarantees no message is duplicated or dropped (messages are
     * atomically drained from [batchQueue] under [batchMutex] before being
     * sent), so invoking it again here on every successful (re)connect is
     * always safe, even if nothing is pending.
     */
    private fun drainPendingBatch() {
        scope.launch {
            try {
                flushBatch()
            } catch (_: Exception) {
                // Best-effort: a failure here re-requeues via flushBatch's
                // own requeueBatch call, and the next successful reconnect
                // retries again — never dropped, never duplicated.
            }
        }
    }

    // -- Heartbeat --

    private fun startHeartbeat() {
        val groupId = consumerConfig.groupId ?: return
        val intervalMs = consumerConfig.heartbeatIntervalMs

        heartbeatJob?.cancel()
        heartbeatJob = scope.launch {
            while (isActive) {
                delay(intervalMs)
                try {
                    val session = wsSession ?: continue
                    val command = outboundJson.encodeToString(
                        HeartbeatCommand(
                            action = "heartbeat",
                            groupId = groupId,
                            sessionTimeoutMs = consumerConfig.sessionTimeoutMs,
                        )
                    )
                    session.send(Frame.Text(command))
                } catch (_: Exception) {
                    // Best-effort; reconnection handles recovery
                }
            }
        }
    }

    // -- Poll-based Consumption --

    /**
     * Poll for messages from all subscribed topics. Returns up to
     * [ConsumerConfig.maxPollRecords] messages within the given timeout.
     *
     * This provides a Kafka-style pull model complementing the callback-based
     * [subscribe] and Flow-based [messages] APIs.
     *
     * ```kotlin
     * client.subscribe("events") { /* no-op handler; use poll instead */ }
     * val batch = client.poll(1000)
     * batch.forEach { msg -> process(msg) }
     * ```
     *
     * @param timeoutMs Maximum time to wait for messages in milliseconds.
     * @return List of messages received within the timeout, up to maxPollRecords.
     */
    suspend fun poll(timeoutMs: Long = 1000): List<StreamlineMessage> {
        val maxRecords = consumerConfig.maxPollRecords
        val results = mutableListOf<StreamlineMessage>()

        // Drain any immediately available messages
        while (results.size < maxRecords) {
            val msg = pollBuffer.tryReceive().getOrNull() ?: break
            results.add(msg)
        }

        // If we got nothing yet, wait up to timeoutMs for the first message
        if (results.isEmpty()) {
            try {
                val first = withTimeout(timeoutMs) {
                    pollBuffer.receive()
                }
                results.add(first)
            } catch (_: kotlinx.coroutines.TimeoutCancellationException) {
                return results
            }

            // Drain more if available
            while (results.size < maxRecords) {
                val msg = pollBuffer.tryReceive().getOrNull() ?: break
                results.add(msg)
            }
        }

        return results
    }

    // -- Batch Produce with Acknowledgments --

    /**
     * Result of a batch produce operation.
     *
     * @property successCount Number of messages successfully sent.
     * @property failureCount Number of messages that failed.
     * @property errors Per-message errors keyed by index.
     */
    data class ProduceResult(
        val successCount: Int,
        val failureCount: Int,
        val errors: Map<Int, String> = emptyMap(),
    )

    /**
     * Send a batch of messages and wait for local send confirmation.
     *
     * Unlike [produce] which batches transparently, this sends all messages
     * immediately and reports per-message delivery status. Per-message
     * success here reflects only that the local WebSocket frame write
     * succeeded — the current produce protocol has no correlated broker
     * acknowledgment, which is why [ProducerConfig.acks] is restricted to
     * [Acks.NONE] (see [ProducerConfig.validate]) and never claimed as a
     * broker-confirmed guarantee.
     *
     * @param messages Messages to produce. Every message's topic and any
     * explicit [StreamlineMessage.partition] is validated before anything in
     * the batch is sent, so a single invalid entry never causes a partial,
     * silently-inconsistent send.
     * @return Aggregated produce result.
     */
    suspend fun produceBatch(messages: List<StreamlineMessage>): ProduceResult {
        producerConfig.validate()
        messages.forEach {
            TopicNameValidator.validate(it.topic)
            validatePartition(it.partition)
        }
        val session = wsSession
            ?: throw NotConnectedException()

        var successCount = 0
        var failureCount = 0
        val errors = mutableMapOf<Int, String>()

        for ((index, message) in messages.withIndex()) {
            try {
                sendWithRetry(message, session)
                successCount++
            } catch (e: Exception) {
                failureCount++
                errors[index] = e.message ?: "Unknown error"
            }
        }

        return ProduceResult(
            successCount = successCount,
            failureCount = failureCount,
            errors = errors,
        )
    }

    // -- Transaction Support --

    private var inTransaction = false
    private val transactionMutex = Mutex()
    private val transactionBuffer = mutableListOf<StreamlineMessage>()

    /**
     * Begin a new transaction. Messages produced after this call are buffered
     * and only sent to the server on [commitTransaction]. If [abortTransaction]
     * is called, all buffered messages are discarded.
     *
     * @throws StreamlineException if a transaction is already active.
     */
    suspend fun beginTransaction() {
        transactionMutex.withLock {
            if (inTransaction) {
                throw StreamlineException(
                    "Transaction already active",
                    errorCode = ErrorCode.PROTOCOL,
                    hint = "Call commitTransaction() or abortTransaction() first",
                )
            }
            inTransaction = true
            transactionBuffer.clear()
        }

        val session = wsSession ?: throw NotConnectedException()
        val command = """{"action":"begin_transaction"}"""
        session.send(Frame.Text(command))
    }

    /**
     * Commit the active transaction, sending all buffered messages to the server.
     *
     * @throws StreamlineException if no transaction is active.
     * @throws StreamlineTimeoutException if the server does not confirm the commit.
     */
    suspend fun commitTransaction() {
        val messagesToSend: List<StreamlineMessage>
        transactionMutex.withLock {
            if (!inTransaction) {
                throw StreamlineException(
                    "No active transaction",
                    errorCode = ErrorCode.PROTOCOL,
                    hint = "Call beginTransaction() first",
                )
            }
            messagesToSend = transactionBuffer.toList()
            transactionBuffer.clear()
            inTransaction = false
        }

        val session = wsSession ?: throw NotConnectedException()

        // Send all buffered messages
        for (message in messagesToSend) {
            sendWithRetry(message, session)
        }

        // Send commit command and wait for acknowledgment
        val command = """{"action":"commit_transaction"}"""
        val response = awaitControlResponse(
            session = session,
            command = command,
            timeoutMessage = "Transaction commit acknowledgement timed out",
        )
        if (response.contains("\"error\"")) {
            throw StreamlineException(
                "Transaction commit failed: $response",
                errorCode = ErrorCode.INTERNAL,
            )
        }
    }

    /**
     * Abort the active transaction, discarding all buffered messages.
     *
     * @throws StreamlineException if no transaction is active.
     */
    suspend fun abortTransaction() {
        transactionMutex.withLock {
            if (!inTransaction) {
                throw StreamlineException(
                    "No active transaction",
                    errorCode = ErrorCode.PROTOCOL,
                    hint = "Call beginTransaction() first",
                )
            }
            transactionBuffer.clear()
            inTransaction = false
        }

        val session = wsSession ?: throw NotConnectedException()
        val command = """{"action":"abort_transaction"}"""
        session.send(Frame.Text(command))
    }

    /**
     * Produce a message within the current transaction. The message is buffered
     * and only sent on [commitTransaction].
     *
     * @throws StreamlineException if no transaction is active.
     */
    suspend fun transactionalProduce(topic: String, key: String? = null, value: String) {
        TopicNameValidator.validate(topic)
        transactionMutex.withLock {
            if (!inTransaction) {
                throw StreamlineException(
                    "No active transaction. Call beginTransaction() first.",
                    errorCode = ErrorCode.PROTOCOL,
                )
            }
            transactionBuffer.add(StreamlineMessage(topic = topic, key = key, value = value))
        }
    }
}


// -- Client-Side Metrics --

/** Snapshot of client-side metrics at a point in time. */
data class ClientMetricsSnapshot(
    val produceCount: Long,
    val produceBytes: Long,
    val produceErrors: Long,
    val produceAvgLatencyMs: Double,
    val consumeCount: Long,
    val consumeBytes: Long,
)

/** Internal mutable metrics collector. */
internal class ClientMetrics {
    private val mutex = Mutex()
    private var produceCount = 0L
    private var produceBytes = 0L
    private var produceErrors = 0L
    private var produceTotalLatencyMs = 0L
    private var consumeCount = 0L
    private var consumeBytes = 0L

    suspend fun recordProduce(bytes: Long, latencyMs: Long) {
        mutex.withLock {
            produceCount++
            produceBytes += bytes
            produceTotalLatencyMs += latencyMs
        }
    }

    suspend fun recordProduceError() {
        mutex.withLock { produceErrors++ }
    }

    suspend fun recordConsume(bytes: Long) {
        mutex.withLock {
            consumeCount++
            consumeBytes += bytes
        }
    }

    fun snapshot(): ClientMetricsSnapshot {
        val avgLatency = if (produceCount > 0) {
            produceTotalLatencyMs.toDouble() / produceCount
        } else {
            0.0
        }
        return ClientMetricsSnapshot(
            produceCount = produceCount,
            produceBytes = produceBytes,
            produceErrors = produceErrors,
            produceAvgLatencyMs = avgLatency,
            consumeCount = consumeCount,
            consumeBytes = consumeBytes,
        )
    }
}


// TlsConfig is defined in Security.kt
