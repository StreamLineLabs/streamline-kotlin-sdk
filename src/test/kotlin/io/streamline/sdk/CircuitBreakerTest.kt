package io.streamline.sdk

import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class CircuitBreakerTest {

    @Test
    fun `starts in CLOSED state`() = runTest {
        val cb = CircuitBreaker()
        assertEquals(CircuitBreakerState.CLOSED, cb.state)
    }

    @Test
    fun `successful calls keep circuit CLOSED`() = runTest {
        val cb = CircuitBreaker(CircuitBreakerConfig(failureThreshold = 3))
        repeat(10) {
            cb.execute { "ok" }
        }
        assertEquals(CircuitBreakerState.CLOSED, cb.state)
    }

    @Test
    fun `opens after reaching failure threshold`() = runTest {
        val cb = CircuitBreaker(CircuitBreakerConfig(failureThreshold = 3))
        repeat(3) {
            runCatching { cb.execute { throw RuntimeException("fail") } }
        }
        assertEquals(CircuitBreakerState.OPEN, cb.state)
    }

    @Test
    fun `rejects calls when OPEN`() = runTest {
        val cb = CircuitBreaker(CircuitBreakerConfig(failureThreshold = 1, resetTimeoutMs = 60_000))
        runCatching { cb.execute { throw RuntimeException("fail") } }
        assertEquals(CircuitBreakerState.OPEN, cb.state)

        assertFailsWith<CircuitBreakerOpenException> {
            cb.execute { "should not run" }
        }
    }

    @Test
    fun `transitions to HALF_OPEN after reset timeout`() = runTest {
        val cb = CircuitBreaker(CircuitBreakerConfig(failureThreshold = 1, resetTimeoutMs = 1))
        runCatching { cb.execute { throw RuntimeException("fail") } }
        assertEquals(CircuitBreakerState.OPEN, cb.state)

        // Wait just past the reset timeout
        kotlinx.coroutines.delay(10)

        cb.execute { "probe" }
        assertEquals(CircuitBreakerState.CLOSED, cb.state)
    }

    @Test
    fun `half-open failure re-opens the circuit`() = runTest {
        val cb = CircuitBreaker(CircuitBreakerConfig(failureThreshold = 1, resetTimeoutMs = 1))
        runCatching { cb.execute { throw RuntimeException("fail") } }

        kotlinx.coroutines.delay(10)

        runCatching { cb.execute { throw RuntimeException("fail again") } }
        assertEquals(CircuitBreakerState.OPEN, cb.state)
    }

    @Test
    fun `reset returns to CLOSED`() = runTest {
        val cb = CircuitBreaker(CircuitBreakerConfig(failureThreshold = 1))
        runCatching { cb.execute { throw RuntimeException("fail") } }
        assertEquals(CircuitBreakerState.OPEN, cb.state)

        cb.reset()
        assertEquals(CircuitBreakerState.CLOSED, cb.state)
    }

    @Test
    fun `failures below threshold keep circuit CLOSED`() = runTest {
        val cb = CircuitBreaker(CircuitBreakerConfig(failureThreshold = 5))
        repeat(4) {
            runCatching { cb.execute { throw RuntimeException("fail") } }
        }
        assertEquals(CircuitBreakerState.CLOSED, cb.state)
    }

    @Test
    fun `success resets failure count`() = runTest {
        val cb = CircuitBreaker(CircuitBreakerConfig(failureThreshold = 3))
        repeat(2) {
            runCatching { cb.execute { throw RuntimeException("fail") } }
        }
        cb.execute { "ok" }
        // Failure count is reset — need 3 more failures to trip
        repeat(2) {
            runCatching { cb.execute { throw RuntimeException("fail") } }
        }
        assertEquals(CircuitBreakerState.CLOSED, cb.state)
    }

    @Test
    fun `CircuitBreakerOpenException has remaining time info`() = runTest {
        val cb = CircuitBreaker(CircuitBreakerConfig(failureThreshold = 1, resetTimeoutMs = 60_000))
        runCatching { cb.execute { throw RuntimeException("fail") } }

        val ex = assertFailsWith<CircuitBreakerOpenException> {
            cb.execute { "no" }
        }
        assertTrue(ex.remainingMs > 0)
        assertEquals(ErrorCode.CIRCUIT_BREAKER_OPEN, ex.code)
        assertTrue(ex.isRetryable)
    }
}
