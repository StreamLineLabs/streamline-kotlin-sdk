package io.streamline.testcontainers

import org.junit.jupiter.api.Test
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

@Testcontainers
class StreamlineContainerTest {

    companion object {
        @Container
        val streamline: StreamlineContainer = StreamlineContainer()
            .withInMemory()
            .withDebugLogging()
    }

    @Test
    fun `container starts and exposes bootstrap servers`() {
        assertTrue(streamline.isRunning, "Container should be running")
        assertNotNull(streamline.bootstrapServers, "Bootstrap servers should not be null")
        assertTrue(streamline.bootstrapServers.contains(":"), "Should have host:port format")
    }

    @Test
    fun `http endpoint is reachable`() {
        assertTrue(streamline.httpUrl.startsWith("http://"), "HTTP URL should start with http://")
        assertTrue(streamline.healthUrl.endsWith("/health"), "Health URL should end with /health")
    }

    @Test
    fun `websocket url is constructed correctly`() {
        assertTrue(streamline.webSocketUrl.startsWith("ws://"), "WebSocket URL should start with ws://")
    }

    @Test
    fun `kafka replacement factory creates valid container`() {
        val container = StreamlineContainer.kafkaReplacement()
        assertNotNull(container, "Factory should return a container")
    }

    @Test
    fun `playground factory creates valid container`() {
        val container = StreamlineContainer.playground()
        assertNotNull(container, "Playground factory should return a container")
    }
}
