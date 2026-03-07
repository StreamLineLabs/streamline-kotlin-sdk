package io.streamline.testcontainers

import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName
import java.time.Duration

/**
 * Testcontainers implementation for the Streamline streaming platform.
 *
 * Starts a real Streamline server in Docker for integration testing.
 * Compatible with JUnit 5 and Kotlin coroutine tests.
 *
 * ```kotlin
 * @Testcontainers
 * class MyIntegrationTest {
 *     companion object {
 *         @Container
 *         val streamline = StreamlineContainer()
 *     }
 *
 *     @Test
 *     fun `produce and consume`() = runBlocking {
 *         val client = StreamlineClient(
 *             StreamlineConfiguration(url = "ws://${streamline.bootstrapServers}")
 *         )
 *         // ...
 *     }
 * }
 * ```
 */
class StreamlineContainer(
    image: DockerImageName = DockerImageName.parse(DEFAULT_IMAGE),
) : GenericContainer<StreamlineContainer>(image) {

    companion object {
        const val DEFAULT_IMAGE = "ghcr.io/streamlinelabs/streamline:0.2.0"
        const val KAFKA_PORT = 9092
        const val HTTP_PORT = 9094
    }

    init {
        withExposedPorts(KAFKA_PORT, HTTP_PORT)
        waitingFor(
            Wait.forHttp("/health")
                .forPort(HTTP_PORT)
                .forStatusCode(200)
                .withStartupTimeout(Duration.ofSeconds(30))
        )
        withEnv("STREAMLINE_LOG_LEVEL", "info")
    }

    /** Kafka-protocol bootstrap address (host:port). */
    val bootstrapServers: String
        get() = "${host}:${getMappedPort(KAFKA_PORT)}"

    /** HTTP API base URL. */
    val httpUrl: String
        get() = "http://${host}:${getMappedPort(HTTP_PORT)}"

    /** Health-check endpoint URL. */
    val healthUrl: String
        get() = "$httpUrl/health"

    /** Prometheus metrics endpoint URL. */
    val metricsUrl: String
        get() = "$httpUrl/metrics"

    /** WebSocket URL for SDK connections. */
    val webSocketUrl: String
        get() = "ws://${host}:${getMappedPort(KAFKA_PORT)}"

    // -- Builder-style configuration --

    /** Set the server log level. */
    fun withLogLevel(level: String): StreamlineContainer = apply {
        withEnv("STREAMLINE_LOG_LEVEL", level)
    }

    /** Enable debug logging. */
    fun withDebugLogging(): StreamlineContainer = withLogLevel("debug")

    /** Enable trace logging. */
    fun withTraceLogging(): StreamlineContainer = withLogLevel("trace")

    /** Enable playground mode with pre-seeded demo topics. */
    fun withPlayground(): StreamlineContainer = apply {
        withEnv("STREAMLINE_PLAYGROUND", "true")
    }

    /** Run with in-memory storage (no persistence). */
    fun withInMemory(): StreamlineContainer = apply {
        withEnv("STREAMLINE_STORAGE_MODE", "memory")
    }

    /** Enable auto-creation of topics on first produce. */
    fun withAutoCreateTopics(): StreamlineContainer = apply {
        withEnv("STREAMLINE_AUTO_CREATE_TOPICS", "true")
    }

    /** Add a custom environment variable. */
    fun withStreamlineEnv(key: String, value: String): StreamlineContainer = apply {
        withEnv(key, value)
    }

    /**
     * Factory that creates a container pre-configured as a Kafka replacement.
     * Useful for migrating existing Kafka tests with minimal changes.
     */
    companion object Factory {
        /** Create a container configured as a drop-in Kafka replacement. */
        fun kafkaReplacement(): StreamlineContainer {
            return StreamlineContainer()
                .withInMemory()
                .withAutoCreateTopics()
                .withLogLevel("warn")
        }

        /** Create a container with demo topics pre-seeded. */
        fun playground(): StreamlineContainer {
            return StreamlineContainer()
                .withInMemory()
                .withPlayground()
        }
    }
}
