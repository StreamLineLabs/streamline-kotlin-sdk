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
import kotlinx.serialization.Serializable

/** Internal response model for offset queries. */
@Serializable
internal data class OffsetResponse(val offset: Long? = null)

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
        if (_state.value == ConnectionState.CONNECTED || _state.value == ConnectionState.CONNECTING) return

        val isReconnecting = _state.value == ConnectionState.RECONNECTING
        if (!isReconnecting) _state.value = ConnectionState.CONNECTING

        try {
            val authHdrs = authHeaders(auth)
            val session = httpClient.webSocketSession(configuration.url) {
                // Apply auth headers to the WebSocket upgrade request
                for ((key, value) in authHdrs) {
                    headers.append(key, value)
                }
                // Legacy authToken support
                if (authHdrs.isEmpty() && configuration.authToken != null) {
                    headers.append("Authorization", "Bearer ${configuration.authToken}")
                }
            }
            wsSession = session
            _state.value = ConnectionState.CONNECTED
            retryCount = 0
            startReceiving(session)
            drainOfflineQueue()
            startAutoCommit()
            startHeartbeat()
        } catch (e: Exception) {
            handleDisconnection(e)
        }
    }

    /** Gracefully close the connection. */
    suspend fun disconnect() {
        heartbeatJob?.cancel()
        heartbeatJob = null
        autoCommitJob?.cancel()
        autoCommitJob = null
        reconnectJob?.cancel()
        reconnectJob = null
        receiveJob?.cancel()
        receiveJob = null

        // Final commit before disconnect
        if (consumerConfig.autoCommit) {
            try { flushAutoCommit() } catch (_: Exception) { /* best-effort */ }
        }

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

    private suspend fun buildPayload(message: StreamlineMessage): String {
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

        val keyJson = message.key?.let { "\"$it\"" } ?: "null"
        val headersJson = if (message.headers.isNotEmpty()) {
            message.headers.entries.joinToString(",", "{", "}") { (k, v) -> "\"$k\":\"$v\"" }
        } else {
            null
        }
        return buildString {
            append("{\"topic\":\"${message.topic}\",\"key\":$keyJson,\"value\":\"${message.value}\"")
            append(",\"acks\":\"$acksStr\"")
            if (compression != CompressionType.NONE) {
                append(",\"compression\":\"${compression.name.lowercase()}\"")
            }
            if (seq != null) {
                append(",\"idempotent\":true,\"sequence\":$seq")
            }
            if (headersJson != null) {
                append(",\"headers\":$headersJson")
            }
            append("}")
        }
    }

    // -- Schema-Aware Production --

    /**
     * Produce a message with schema validation. The value is validated against
     * the latest schema registered for the topic's value subject (topic-value)
     * before sending.
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
        // Validate compatibility before sending
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
        subscriptionsMutex.withLock {
            subscriptions[topic] = handler
        }

        val session = wsSession ?: return
        val groupId = consumerConfig.groupId
        val resetStrategy = consumerConfig.autoOffsetReset.name.lowercase()
        val command = buildString {
            append("{\"action\":\"subscribe\",\"topic\":\"$topic\"")
            append(",\"auto_offset_reset\":\"$resetStrategy\"")
            if (groupId != null) {
                append(",\"group_id\":\"$groupId\"")
                append(",\"session_timeout_ms\":${consumerConfig.sessionTimeoutMs}")
                append(",\"heartbeat_interval_ms\":${consumerConfig.heartbeatIntervalMs}")
            }
            append("}")
        }
        session.send(Frame.Text(command))

        // Apply offset reset strategy for non-grouped consumers
        if (groupId == null) {
            when (consumerConfig.autoOffsetReset) {
                OffsetReset.EARLIEST -> seekToBeginning(topic)
                OffsetReset.LATEST -> { /* server default is latest */ }
                OffsetReset.NONE -> { /* no reset — fail if no committed offset */ }
            }
        }
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

        val entries = offsets.entries.joinToString(",") { (key, offset) ->
            """{"topicPartition":"$key","offset":$offset}"""
        }
        val command = """{"action":"commit_offsets","offsets":[$entries]}"""
        session.send(Frame.Text(command))

        // Wait for server acknowledgement
        try {
            val response = withTimeout(configuration.timeoutMs) {
                session.incoming.receive()
            }
            when (response) {
                is Frame.Text -> {
                    val text = response.readText()
                    if (text.contains("\"error\"")) {
                        throw StreamlineException(
                            "Offset commit rejected by server: $text",
                            errorCode = ErrorCode.INTERNAL,
                        )
                    }
                }
                else -> { /* non-text acknowledgement accepted */ }
            }
        } catch (e: kotlinx.coroutines.TimeoutCancellationException) {
            throw StreamlineTimeoutException("Offset commit acknowledgement timed out")
        }
    }

    /**
     * Seek the consumer to a specific offset for a topic partition.
     */
    suspend fun seekToOffset(topic: String, partition: Int, offset: Long) {
        val session = wsSession
            ?: throw NotConnectedException()

        val command = """{"action":"seek","topic":"$topic","partition":$partition,"offset":$offset}"""
        session.send(Frame.Text(command))
    }

    /** Seek the consumer to the beginning of all partitions for the given topic. */
    suspend fun seekToBeginning(topic: String) {
        val session = wsSession
            ?: throw NotConnectedException()

        val command = """{"action":"seek_to_beginning","topic":"$topic"}"""
        session.send(Frame.Text(command))
    }

    /** Seek the consumer to the end (latest) of all partitions for the given topic. */
    suspend fun seekToEnd(topic: String) {
        val session = wsSession
            ?: throw NotConnectedException()

        val command = """{"action":"seek_to_end","topic":"$topic"}"""
        session.send(Frame.Text(command))
    }

    /**
     * Get the current consumer position (next offset to be read) for a topic partition.
     *
     * @return The current position, or null if not available.
     */
    suspend fun position(topic: String, partition: Int): Long? {
        val session = wsSession
            ?: throw NotConnectedException()

        val command = """{"action":"position","topic":"$topic","partition":$partition}"""
        session.send(Frame.Text(command))

        val response = withTimeout(configuration.timeoutMs) {
            session.incoming.receive()
        }
        return when (response) {
            is Frame.Text -> {
                val text = response.readText()
                try {
                    val result = json.decodeFromString<OffsetResponse>(text)
                    result.offset
                } catch (_: Exception) {
                    null
                }
            }
            else -> null
        }
    }

    /**
     * Get the last committed offset for a topic partition.
     *
     * @return The committed offset, or null if no offset has been committed.
     */
    suspend fun committed(topic: String, partition: Int): Long? {
        val session = wsSession
            ?: throw NotConnectedException()

        val command = """{"action":"committed","topic":"$topic","partition":$partition}"""
        session.send(Frame.Text(command))

        val response = withTimeout(configuration.timeoutMs) {
            session.incoming.receive()
        }
        return when (response) {
            is Frame.Text -> {
                val text = response.readText()
                try {
                    val result = json.decodeFromString<OffsetResponse>(text)
                    result.offset
                } catch (_: Exception) {
                    null
                }
            }
            else -> null
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

        // Track consumed offsets for auto-commit
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

        // Feed into poll buffer (non-blocking, drops if full)
        pollBuffer.trySend(message)

        val handler = subscriptionsMutex.withLock { subscriptions[message.topic] }
        handler?.invoke(message)
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
                    val command = """{"action":"heartbeat","group_id":"$groupId","session_timeout_ms":${consumerConfig.sessionTimeoutMs}}"""
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
     * Send a batch of messages and wait for acknowledgments based on
     * [ProducerConfig.acks] setting. Unlike [produce] which batches
     * transparently, this sends all messages immediately and reports
     * per-message delivery status.
     *
     * @param messages Messages to produce.
     * @return Aggregated produce result.
     */
    suspend fun produceBatch(messages: List<StreamlineMessage>): ProduceResult {
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
        session.send(Frame.Text(command))

        try {
            val response = withTimeout(configuration.timeoutMs) {
                session.incoming.receive()
            }
            when (response) {
                is Frame.Text -> {
                    val text = response.readText()
                    if (text.contains("\"error\"")) {
                        throw StreamlineException(
                            "Transaction commit failed: $text",
                            errorCode = ErrorCode.INTERNAL,
                        )
                    }
                }
                else -> { /* non-text ack accepted */ }
            }
        } catch (e: kotlinx.coroutines.TimeoutCancellationException) {
            throw StreamlineTimeoutException("Transaction commit acknowledgement timed out")
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
