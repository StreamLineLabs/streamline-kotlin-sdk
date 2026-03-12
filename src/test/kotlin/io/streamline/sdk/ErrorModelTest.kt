package io.streamline.sdk

import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds

class ErrorModelTest {

    // -- ErrorCode enum --

    @Test
    fun `all error codes are present`() {
        val codes = ErrorCode.entries
        assertEquals(12, codes.size)
        assertTrue(codes.contains(ErrorCode.CONNECTION))
        assertTrue(codes.contains(ErrorCode.TIMEOUT))
        assertTrue(codes.contains(ErrorCode.AUTHENTICATION))
        assertTrue(codes.contains(ErrorCode.AUTHORIZATION))
        assertTrue(codes.contains(ErrorCode.TOPIC_NOT_FOUND))
        assertTrue(codes.contains(ErrorCode.PARTITION_NOT_FOUND))
        assertTrue(codes.contains(ErrorCode.PROTOCOL))
        assertTrue(codes.contains(ErrorCode.SERIALIZATION))
        assertTrue(codes.contains(ErrorCode.SCHEMA))
        assertTrue(codes.contains(ErrorCode.CONFIGURATION))
        assertTrue(codes.contains(ErrorCode.INTERNAL))
        assertTrue(codes.contains(ErrorCode.CIRCUIT_OPEN))
    }

    @Test
    fun `retryable error codes`() {
        assertTrue(ErrorCode.CONNECTION.defaultRetryable)
        assertTrue(ErrorCode.TIMEOUT.defaultRetryable)
        assertTrue(ErrorCode.INTERNAL.defaultRetryable)
    }

    @Test
    fun `non-retryable error codes`() {
        assertFalse(ErrorCode.AUTHENTICATION.defaultRetryable)
        assertFalse(ErrorCode.AUTHORIZATION.defaultRetryable)
        assertFalse(ErrorCode.TOPIC_NOT_FOUND.defaultRetryable)
        assertFalse(ErrorCode.PARTITION_NOT_FOUND.defaultRetryable)
        assertFalse(ErrorCode.PROTOCOL.defaultRetryable)
        assertFalse(ErrorCode.SERIALIZATION.defaultRetryable)
        assertFalse(ErrorCode.SCHEMA.defaultRetryable)
        assertFalse(ErrorCode.CONFIGURATION.defaultRetryable)
        assertFalse(ErrorCode.CIRCUIT_OPEN.defaultRetryable)
    }

    // -- StreamlineException base --

    @Test
    fun `base exception defaults to INTERNAL`() {
        val ex = StreamlineException("something went wrong")
        assertEquals(ErrorCode.INTERNAL, ex.errorCode)
        assertTrue(ex.retryable)
        assertTrue(ex.isRetryable())
        assertNull(ex.hint)
        assertNull(ex.cause)
    }

    @Test
    fun `base exception with custom error code`() {
        val ex = StreamlineException(
            message = "custom error",
            errorCode = ErrorCode.CONFIGURATION,
            hint = "fix your config",
        )
        assertEquals(ErrorCode.CONFIGURATION, ex.errorCode)
        assertFalse(ex.isRetryable())
        assertEquals("fix your config", ex.hint)
    }

    @Test
    fun `base exception retryable override`() {
        val ex = StreamlineException(
            message = "force retryable",
            errorCode = ErrorCode.AUTHENTICATION,
            retryable = true,
        )
        assertEquals(ErrorCode.AUTHENTICATION, ex.errorCode)
        assertTrue(ex.isRetryable())
    }

    // -- Subclass error codes and retryability --

    @Test
    fun `NotConnectedException has correct error code`() {
        val ex = NotConnectedException()
        assertEquals(ErrorCode.CONNECTION, ex.errorCode)
        assertTrue(ex.isRetryable())
        assertEquals("Client is not connected", ex.message)
        assertNotNull(ex.hint)
    }

    @Test
    fun `ConnectionFailedException has correct error code`() {
        val cause = RuntimeException("network error")
        val ex = ConnectionFailedException("connection refused", cause)
        assertEquals(ErrorCode.CONNECTION, ex.errorCode)
        assertTrue(ex.isRetryable())
        assertEquals(cause, ex.cause)
        assertNotNull(ex.hint)
    }

    @Test
    fun `AuthenticationFailedException has correct error code`() {
        val ex = AuthenticationFailedException("bad token")
        assertEquals(ErrorCode.AUTHENTICATION, ex.errorCode)
        assertFalse(ex.isRetryable())
        assertNotNull(ex.hint)
    }

    @Test
    fun `AuthorizationFailedException has correct error code`() {
        val ex = AuthorizationFailedException("forbidden")
        assertEquals(ErrorCode.AUTHORIZATION, ex.errorCode)
        assertFalse(ex.isRetryable())
        assertNotNull(ex.hint)
    }

    @Test
    fun `StreamlineTimeoutException has correct error code`() {
        val ex = StreamlineTimeoutException()
        assertEquals(ErrorCode.TIMEOUT, ex.errorCode)
        assertTrue(ex.isRetryable())
        assertEquals("Operation timed out", ex.message)
        assertNotNull(ex.hint)
    }

    @Test
    fun `StreamlineTimeoutException custom message`() {
        val ex = StreamlineTimeoutException("connect timed out after 5s")
        assertEquals("connect timed out after 5s", ex.message)
        assertEquals(ErrorCode.TIMEOUT, ex.errorCode)
    }

    @Test
    fun `TopicNotFoundException has correct error code`() {
        val ex = TopicNotFoundException("my-topic")
        assertEquals(ErrorCode.TOPIC_NOT_FOUND, ex.errorCode)
        assertFalse(ex.isRetryable())
        assertEquals("Topic not found: my-topic", ex.message)
        assertNotNull(ex.hint)
    }

    @Test
    fun `PartitionNotFoundException has correct error code`() {
        val ex = PartitionNotFoundException("events", 7)
        assertEquals(ErrorCode.PARTITION_NOT_FOUND, ex.errorCode)
        assertFalse(ex.isRetryable())
        assertTrue(ex.message!!.contains("7"))
        assertTrue(ex.message!!.contains("events"))
    }

    @Test
    fun `ProtocolException has correct error code`() {
        val ex = ProtocolException("unexpected frame type")
        assertEquals(ErrorCode.PROTOCOL, ex.errorCode)
        assertFalse(ex.isRetryable())
        assertNotNull(ex.hint)
    }

    @Test
    fun `SerializationException has correct error code`() {
        val cause = RuntimeException("bad JSON")
        val ex = SerializationException("failed to deserialize", cause)
        assertEquals(ErrorCode.SERIALIZATION, ex.errorCode)
        assertFalse(ex.isRetryable())
        assertEquals(cause, ex.cause)
    }

    @Test
    fun `OfflineQueueFullException is not retryable`() {
        val ex = OfflineQueueFullException()
        assertEquals(ErrorCode.INTERNAL, ex.errorCode)
        assertFalse(ex.isRetryable())
        assertNotNull(ex.hint)
    }

    @Test
    fun `AdminOperationException has correct error code`() {
        val ex = AdminOperationException("request failed")
        assertEquals(ErrorCode.INTERNAL, ex.errorCode)
        assertTrue(ex.isRetryable())
    }

    @Test
    fun `QueryException has correct error code`() {
        val ex = QueryException("invalid SQL")
        assertEquals(ErrorCode.INTERNAL, ex.errorCode)
        assertTrue(ex.isRetryable())
    }

    @Test
    fun `SchemaRegistryException has correct error code`() {
        val ex = SchemaRegistryException("schema not found")
        assertEquals(ErrorCode.SCHEMA, ex.errorCode)
        assertFalse(ex.isRetryable())
    }

    @Test
    fun `CircuitOpenException has correct error code`() {
        val ex = CircuitOpenException()
        assertEquals(ErrorCode.CIRCUIT_OPEN, ex.errorCode)
        assertFalse(ex.isRetryable())
        assertNotNull(ex.hint)
    }

    @Test
    fun `ConfigurationException has correct error code`() {
        val ex = ConfigurationException("invalid URL format")
        assertEquals(ErrorCode.CONFIGURATION, ex.errorCode)
        assertFalse(ex.isRetryable())
        assertNotNull(ex.hint)
    }

    // -- Exception hierarchy is preserved --

    @Test
    fun `all exceptions extend StreamlineException`() {
        assertTrue(NotConnectedException() is StreamlineException)
        assertTrue(ConnectionFailedException("x") is StreamlineException)
        assertTrue(AuthenticationFailedException("x") is StreamlineException)
        assertTrue(AuthorizationFailedException("x") is StreamlineException)
        assertTrue(StreamlineTimeoutException() is StreamlineException)
        assertTrue(TopicNotFoundException("x") is StreamlineException)
        assertTrue(PartitionNotFoundException("x", 0) is StreamlineException)
        assertTrue(ProtocolException("x") is StreamlineException)
        assertTrue(SerializationException("x") is StreamlineException)
        assertTrue(OfflineQueueFullException() is StreamlineException)
        assertTrue(AdminOperationException("x") is StreamlineException)
        assertTrue(QueryException("x") is StreamlineException)
        assertTrue(SchemaRegistryException("x") is StreamlineException)
        assertTrue(CircuitOpenException() is StreamlineException)
        assertTrue(ConfigurationException("x") is StreamlineException)
    }

    @Test
    fun `all exceptions extend kotlin Exception`() {
        assertTrue(NotConnectedException() is Exception)
        assertTrue(CircuitOpenException() is Exception)
        assertTrue(ConfigurationException("x") is Exception)
    }
}
