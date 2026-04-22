package io.streamline.sdk

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import java.util.UUID

/**
 * Lightweight telemetry interface that can be backed by OpenTelemetry or
 * a no-op implementation. This avoids a hard dependency on the OTel SDK.
 */
interface Telemetry {
    /** Start a new span and return a context token to close it. */
    fun startSpan(name: String, attributes: Map<String, String> = emptyMap()): SpanHandle

    /** Generate a W3C traceparent header value for propagation. */
    fun traceparent(): String?
}

/** Handle to an active span. Call [end] when the operation completes. */
interface SpanHandle {
    fun setAttribute(key: String, value: String)
    fun setError(error: Throwable)
    fun end()
}

// -- No-Op Implementation (default) --

internal object NoOpTelemetry : Telemetry {
    override fun startSpan(name: String, attributes: Map<String, String>): SpanHandle = NoOpSpan
    override fun traceparent(): String? = null
}

private object NoOpSpan : SpanHandle {
    override fun setAttribute(key: String, value: String) {}
    override fun setError(error: Throwable) {}
    override fun end() {}
}

// -- Console Telemetry (development/debugging) --

/**
 * Simple telemetry implementation that logs spans to stdout.
 * Useful for development and debugging without requiring OpenTelemetry.
 */
class ConsoleTelemetry(private val serviceName: String = "streamline-kotlin-sdk") : Telemetry {

    private val traceId = UUID.randomUUID().toString().replace("-", "")

    override fun startSpan(name: String, attributes: Map<String, String>): SpanHandle {
        val spanId = UUID.randomUUID().toString().replace("-", "").take(16)
        val startTime = System.nanoTime()
        println("[$serviceName] SPAN START: $name ${attributes.entries.joinToString { "${it.key}=${it.value}" }}")
        return ConsoleSpan(serviceName, name, spanId, startTime)
    }

    override fun traceparent(): String {
        val spanId = UUID.randomUUID().toString().replace("-", "").take(16)
        return "00-$traceId-$spanId-01"
    }

    private class ConsoleSpan(
        private val serviceName: String,
        private val name: String,
        private val spanId: String,
        private val startTime: Long,
    ) : SpanHandle {
        private val attrs = mutableMapOf<String, String>()
        private var error: Throwable? = null

        override fun setAttribute(key: String, value: String) {
            attrs[key] = value
        }

        override fun setError(error: Throwable) {
            this.error = error
        }

        override fun end() {
            val durationMs = (System.nanoTime() - startTime) / 1_000_000
            val status = if (error != null) "ERROR(${error!!.message})" else "OK"
            println("[$serviceName] SPAN END: $name duration=${durationMs}ms status=$status $spanId")
        }
    }
}

// -- Instrumented Wrappers --

/**
 * Wraps a [StreamlineClient] with telemetry instrumentation.
 * Every produce, subscribe, and message reception creates a span.
 *
 * ```kotlin
 * val telemetry = ConsoleTelemetry()
 * val client = StreamlineClient(config)
 * val traced = TracedClient(client, telemetry)
 * traced.produce("events", value = "hello")
 * ```
 */
class TracedClient(
    private val delegate: StreamlineClient,
    private val telemetry: Telemetry,
) {
    val state get() = delegate.state

    suspend fun connect() {
        val span = telemetry.startSpan("streamline.connect")
        try {
            delegate.connect()
            span.setAttribute("status", "connected")
        } catch (e: Exception) {
            span.setError(e)
            throw e
        } finally {
            span.end()
        }
    }

    suspend fun disconnect() {
        val span = telemetry.startSpan("streamline.disconnect")
        try {
            delegate.disconnect()
        } finally {
            span.end()
        }
    }

    suspend fun produce(topic: String, key: String? = null, value: String) {
        TopicNameValidator.validate(topic)
        val span = telemetry.startSpan(
            "streamline.produce",
            buildMap {
                put("messaging.system", "streamline")
                put("messaging.destination.name", topic)
                if (key != null) put("messaging.message.key", key)
            },
        )
        try {
            delegate.produce(topic, key, value)
        } catch (e: Exception) {
            span.setError(e)
            throw e
        } finally {
            span.end()
        }
    }

    suspend fun subscribe(topic: String, handler: MessageHandler) {
        TopicNameValidator.validate(topic)
        val span = telemetry.startSpan(
            "streamline.subscribe",
            mapOf("messaging.destination.name" to topic),
        )
        try {
            delegate.subscribe(topic) { message ->
                val receiveSpan = telemetry.startSpan(
                    "streamline.receive",
                    buildMap {
                        put("messaging.destination.name", message.topic)
                        message.key?.let { put("messaging.message.key", it) }
                        message.offset?.let { put("messaging.message.offset", it.toString()) }
                    },
                )
                try {
                    handler(message)
                } catch (e: Exception) {
                    receiveSpan.setError(e)
                    throw e
                } finally {
                    receiveSpan.end()
                }
            }
        } catch (e: Exception) {
            span.setError(e)
            throw e
        } finally {
            span.end()
        }
    }

    /** Flow-based consumption with per-message tracing. */
    fun messages(topic: String): Flow<StreamlineMessage> {
        return delegate.messages(topic).map { message ->
            val span = telemetry.startSpan(
                "streamline.receive",
                buildMap {
                    put("messaging.destination.name", message.topic)
                    message.key?.let { put("messaging.message.key", it) }
                },
            )
            span.end()
            message
        }
    }

    fun close() = delegate.close()
}

/**
 * Wraps an [AdminClient] with telemetry instrumentation.
 *
 * ```kotlin
 * val traced = TracedAdminClient(adminClient, ConsoleTelemetry())
 * traced.createTopic("events", partitions = 3)
 * ```
 */
class TracedAdminClient(
    private val delegate: AdminClient,
    private val telemetry: Telemetry,
) {
    suspend fun listTopics(): List<TopicInfo> = traced("streamline.admin.list_topics") {
        delegate.listTopics()
    }

    suspend fun describeTopic(name: String): TopicDescription = traced("streamline.admin.describe_topic",
        mapOf("messaging.destination.name" to name),
    ) {
        delegate.describeTopic(name)
    }

    suspend fun createTopic(
        name: String,
        partitions: Int = 1,
        replicationFactor: Int = 1,
        config: Map<String, String> = emptyMap(),
    ) = traced("streamline.admin.create_topic",
        mapOf("messaging.destination.name" to name, "topic.partitions" to partitions.toString()),
    ) {
        TopicNameValidator.validate(name)
        delegate.createTopic(name, partitions, replicationFactor, config)
    }

    suspend fun deleteTopic(name: String) = traced("streamline.admin.delete_topic",
        mapOf("messaging.destination.name" to name),
    ) {
        TopicNameValidator.validate(name)
        delegate.deleteTopic(name)
    }

    suspend fun listConsumerGroups(): List<ConsumerGroup> = traced("streamline.admin.list_groups") {
        delegate.listConsumerGroups()
    }

    suspend fun describeConsumerGroup(groupId: String): ConsumerGroupDescription =
        traced("streamline.admin.describe_group", mapOf("messaging.consumer.group.name" to groupId)) {
            delegate.describeConsumerGroup(groupId)
        }

    suspend fun query(sql: String): QueryResult = traced("streamline.admin.query",
        mapOf("db.statement" to sql),
    ) {
        delegate.query(sql)
    }

    suspend fun serverInfo(): ServerInfo = traced("streamline.admin.server_info") {
        delegate.serverInfo()
    }

    fun close() = delegate.close()

    private suspend inline fun <T> traced(
        name: String,
        attributes: Map<String, String> = emptyMap(),
        block: () -> T,
    ): T {
        val span = telemetry.startSpan(name, attributes)
        return try {
            block().also { span.end() }
        } catch (e: Exception) {
            span.setError(e)
            span.end()
            throw e
        }
    }
}
