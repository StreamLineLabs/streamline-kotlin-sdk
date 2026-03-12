package io.streamline.sdk

import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import java.util.concurrent.atomic.AtomicLong

class CircuitBreakerTest {

    private fun fakeClock(): Pair<AtomicLong, () -> Long> {
        val time = AtomicLong(0L)
        return time to { time.get() }
    }

    // -- Initial state --

    @Test
    fun `initial state is CLOSED`() = runTest {
        val cb = CircuitBreaker()
        assertEquals(CircuitState.CLOSED, cb.state())
    }

    @Test
    fun `initial counts are zero`() = runTest {
        val cb = CircuitBreaker()
        val counts = cb.counts()
        assertEquals(0L, counts.totalSuccesses)
        assertEquals(0L, counts.totalFailures)
        assertEquals(0, counts.consecutiveSuccesses)
        assertEquals(0, counts.consecutiveFailures)
    }

    // -- CLOSED state --

    @Test
    fun `allow succeeds when CLOSED`() = runTest {
        val cb = CircuitBreaker()
        cb.allow() // should not throw
    }

    @Test
    fun `recordSuccess increments counters`() = runTest {
        val cb = CircuitBreaker()
        cb.recordSuccess()
        cb.recordSuccess()
        val counts = cb.counts()
        assertEquals(2L, counts.totalSuccesses)
        assertEquals(2, counts.consecutiveSuccesses)
        assertEquals(0, counts.consecutiveFailures)
    }

    @Test
    fun `recordFailure increments counters`() = runTest {
        val cb = CircuitBreaker()
        cb.recordFailure()
        val counts = cb.counts()
        assertEquals(1L, counts.totalFailures)
        assertEquals(1, counts.consecutiveFailures)
        assertEquals(0, counts.consecutiveSuccesses)
    }

    @Test
    fun `success resets consecutive failures`() = runTest {
        val cb = CircuitBreaker()
        cb.recordFailure()
        cb.recordFailure()
        cb.recordSuccess()
        val counts = cb.counts()
        assertEquals(1, counts.consecutiveSuccesses)
        assertEquals(0, counts.consecutiveFailures)
    }

    @Test
    fun `failure resets consecutive successes`() = runTest {
        val cb = CircuitBreaker()
        cb.recordSuccess()
        cb.recordSuccess()
        cb.recordFailure()
        val counts = cb.counts()
        assertEquals(0, counts.consecutiveSuccesses)
        assertEquals(1, counts.consecutiveFailures)
    }

    // -- CLOSED → OPEN transition --

    @Test
    fun `opens after reaching failure threshold`() = runTest {
        val cb = CircuitBreaker(CircuitBreakerConfig(failureThreshold = 3))
        cb.recordFailure()
        cb.recordFailure()
        assertEquals(CircuitState.CLOSED, cb.state())
        cb.recordFailure()
        assertEquals(CircuitState.OPEN, cb.state())
    }

    @Test
    fun `allow throws CircuitOpenException when OPEN`() = runTest {
        val cb = CircuitBreaker(CircuitBreakerConfig(failureThreshold = 2))
        cb.recordFailure()
        cb.recordFailure()
        assertEquals(CircuitState.OPEN, cb.state())

        val ex = assertFailsWith<CircuitOpenException> { cb.allow() }
        assertEquals(ErrorCode.CIRCUIT_OPEN, ex.errorCode)
        assertTrue(ex is StreamlineException)
    }

    // -- OPEN → HALF_OPEN transition --

    @Test
    fun `transitions to HALF_OPEN after open timeout`() = runTest {
        val (time, clock) = fakeClock()
        val cb = CircuitBreaker(
            CircuitBreakerConfig(failureThreshold = 1, openTimeout = 50.milliseconds),
            clock = clock,
        )
        cb.recordFailure()
        assertEquals(CircuitState.OPEN, cb.state())

        time.set(100) // advance past open timeout
        cb.allow() // should succeed, triggering transition
        assertEquals(CircuitState.HALF_OPEN, cb.state())
    }

    // -- HALF_OPEN state --

    @Test
    fun `HALF_OPEN limits probe requests`() = runTest {
        val (time, clock) = fakeClock()
        val cb = CircuitBreaker(
            CircuitBreakerConfig(
                failureThreshold = 1,
                openTimeout = 50.milliseconds,
                halfOpenMaxRequests = 2,
            ),
            clock = clock,
        )
        cb.recordFailure()
        time.set(100)

        cb.allow() // transitions to HALF_OPEN, probe 1
        cb.allow() // probe 2

        assertFailsWith<CircuitOpenException> { cb.allow() } // over limit
    }

    @Test
    fun `HALF_OPEN closes after success threshold`() = runTest {
        val (time, clock) = fakeClock()
        val cb = CircuitBreaker(
            CircuitBreakerConfig(
                failureThreshold = 1,
                successThreshold = 2,
                openTimeout = 50.milliseconds,
                halfOpenMaxRequests = 5,
            ),
            clock = clock,
        )
        cb.recordFailure()
        time.set(100)
        cb.allow() // transition to HALF_OPEN

        cb.recordSuccess()
        assertEquals(CircuitState.HALF_OPEN, cb.state())
        cb.recordSuccess()
        assertEquals(CircuitState.CLOSED, cb.state())
    }

    @Test
    fun `HALF_OPEN reopens on failure`() = runTest {
        val (time, clock) = fakeClock()
        val cb = CircuitBreaker(
            CircuitBreakerConfig(failureThreshold = 1, openTimeout = 50.milliseconds),
            clock = clock,
        )
        cb.recordFailure()
        time.set(100)
        cb.allow() // transition to HALF_OPEN
        assertEquals(CircuitState.HALF_OPEN, cb.state())

        cb.recordFailure() // should reopen
        assertEquals(CircuitState.OPEN, cb.state())
    }

    // -- Reset --

    @Test
    fun `reset returns to CLOSED with zero counts`() = runTest {
        val cb = CircuitBreaker(CircuitBreakerConfig(failureThreshold = 2))
        cb.recordFailure()
        cb.recordFailure()
        assertEquals(CircuitState.OPEN, cb.state())

        cb.reset()
        assertEquals(CircuitState.CLOSED, cb.state())
        val counts = cb.counts()
        assertEquals(0L, counts.totalSuccesses)
        assertEquals(0L, counts.totalFailures)
        assertEquals(0, counts.consecutiveSuccesses)
        assertEquals(0, counts.consecutiveFailures)
    }

    @Test
    fun `reset from HALF_OPEN`() = runTest {
        val (time, clock) = fakeClock()
        val cb = CircuitBreaker(
            CircuitBreakerConfig(failureThreshold = 1, openTimeout = 50.milliseconds),
            clock = clock,
        )
        cb.recordFailure()
        time.set(100)
        cb.allow()
        assertEquals(CircuitState.HALF_OPEN, cb.state())

        cb.reset()
        assertEquals(CircuitState.CLOSED, cb.state())
    }

    // -- Default config --

    @Test
    fun `default config values`() {
        val config = CircuitBreakerConfig()
        assertEquals(5, config.failureThreshold)
        assertEquals(2, config.successThreshold)
        assertEquals(30_000.milliseconds, config.openTimeout)
        assertEquals(3, config.halfOpenMaxRequests)
    }

    @Test
    fun `custom config values`() {
        val config = CircuitBreakerConfig(
            failureThreshold = 10,
            successThreshold = 3,
            openTimeout = 60_000.milliseconds,
            halfOpenMaxRequests = 5,
        )
        assertEquals(10, config.failureThreshold)
        assertEquals(3, config.successThreshold)
        assertEquals(60_000.milliseconds, config.openTimeout)
        assertEquals(5, config.halfOpenMaxRequests)
    }

    // -- CircuitState enum --

    @Test
    fun `all circuit states are present`() {
        val states = CircuitState.entries
        assertEquals(3, states.size)
        assertTrue(states.contains(CircuitState.CLOSED))
        assertTrue(states.contains(CircuitState.OPEN))
        assertTrue(states.contains(CircuitState.HALF_OPEN))
    }

    // -- Total counts accumulate across transitions --

    @Test
    fun `total counts accumulate through state transitions`() = runTest {
        val (time, clock) = fakeClock()
        val cb = CircuitBreaker(
            CircuitBreakerConfig(
                failureThreshold = 2,
                successThreshold = 1,
                openTimeout = 50.milliseconds,
            ),
            clock = clock,
        )

        cb.recordSuccess() // CLOSED
        cb.recordFailure()
        cb.recordFailure() // → OPEN

        time.set(100)
        cb.allow() // → HALF_OPEN
        cb.recordSuccess() // → CLOSED

        val counts = cb.counts()
        assertEquals(2L, counts.totalSuccesses)
        assertEquals(2L, counts.totalFailures)
    }
}
