package io.streamline.sdk

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/** Possible states of a [CircuitBreaker]. */
enum class CircuitState {
    /** Normal operation — requests flow through. */
    CLOSED,
    /** Requests are rejected immediately after repeated failures. */
    OPEN,
    /** A limited number of probe requests are allowed to test recovery. */
    HALF_OPEN,
}

/**
 * Configuration for a [CircuitBreaker].
 *
 * @property failureThreshold Consecutive failures required to open the circuit.
 * @property successThreshold Consecutive successes in HALF_OPEN required to close the circuit.
 * @property openTimeout How long the circuit stays OPEN before transitioning to HALF_OPEN.
 * @property halfOpenMaxRequests Maximum concurrent probe requests allowed in HALF_OPEN state.
 */
data class CircuitBreakerConfig(
    val failureThreshold: Int = 5,
    val successThreshold: Int = 2,
    val openTimeout: Duration = 30.seconds,
    val halfOpenMaxRequests: Int = 3,
)

/** Snapshot of the circuit breaker's internal counters. */
data class CircuitBreakerCounts(
    val totalSuccesses: Long,
    val totalFailures: Long,
    val consecutiveSuccesses: Int,
    val consecutiveFailures: Int,
)

/**
 * A coroutine-safe circuit breaker that protects downstream calls from cascading failures.
 *
 * Usage:
 * ```kotlin
 * val cb = CircuitBreaker()
 * cb.allow() // throws CircuitOpenException if open
 * try {
 *     performOperation()
 *     cb.recordSuccess()
 * } catch (e: Exception) {
 *     cb.recordFailure()
 *     throw e
 * }
 * ```
 */
class CircuitBreaker(
    private val config: CircuitBreakerConfig = CircuitBreakerConfig(),
    private val clock: () -> Long = System::currentTimeMillis,
) {

    private val mutex = Mutex()

    private var _state: CircuitState = CircuitState.CLOSED
    private var openedAtMillis: Long = 0L
    private var halfOpenRequests: Int = 0

    private var totalSuccesses: Long = 0
    private var totalFailures: Long = 0
    private var consecutiveSuccesses: Int = 0
    private var consecutiveFailures: Int = 0

    /**
     * Check whether a request is allowed to proceed.
     *
     * @throws CircuitOpenException if the circuit is OPEN and the open timeout has not elapsed.
     */
    suspend fun allow() {
        mutex.withLock {
            when (_state) {
                CircuitState.CLOSED -> { /* always allowed */ }
                CircuitState.OPEN -> {
                    val elapsed = clock() - openedAtMillis
                    if (elapsed >= config.openTimeout.inWholeMilliseconds) {
                        transitionTo(CircuitState.HALF_OPEN)
                        halfOpenRequests = 1
                    } else {
                        throw CircuitOpenException()
                    }
                }
                CircuitState.HALF_OPEN -> {
                    if (halfOpenRequests >= config.halfOpenMaxRequests) {
                        throw CircuitOpenException("Circuit breaker is half-open and at max probe requests")
                    }
                    halfOpenRequests++
                }
            }
        }
    }

    /** Record a successful operation. May transition HALF_OPEN → CLOSED. */
    suspend fun recordSuccess() {
        mutex.withLock {
            totalSuccesses++
            consecutiveSuccesses++
            consecutiveFailures = 0

            if (_state == CircuitState.HALF_OPEN && consecutiveSuccesses >= config.successThreshold) {
                transitionTo(CircuitState.CLOSED)
            }
        }
    }

    /** Record a failed operation. May transition CLOSED → OPEN or HALF_OPEN → OPEN. */
    suspend fun recordFailure() {
        mutex.withLock {
            totalFailures++
            consecutiveFailures++
            consecutiveSuccesses = 0

            when (_state) {
                CircuitState.CLOSED -> {
                    if (consecutiveFailures >= config.failureThreshold) {
                        transitionTo(CircuitState.OPEN)
                    }
                }
                CircuitState.HALF_OPEN -> {
                    transitionTo(CircuitState.OPEN)
                }
                CircuitState.OPEN -> { /* already open */ }
            }
        }
    }

    /** Returns the current circuit state. */
    suspend fun state(): CircuitState = mutex.withLock { _state }

    /** Manually reset the circuit breaker to CLOSED with zeroed counters. */
    suspend fun reset() {
        mutex.withLock {
            _state = CircuitState.CLOSED
            openedAtMillis = 0L
            halfOpenRequests = 0
            totalSuccesses = 0
            totalFailures = 0
            consecutiveSuccesses = 0
            consecutiveFailures = 0
        }
    }

    /** Returns a snapshot of the internal counters. */
    suspend fun counts(): CircuitBreakerCounts = mutex.withLock {
        CircuitBreakerCounts(
            totalSuccesses = totalSuccesses,
            totalFailures = totalFailures,
            consecutiveSuccesses = consecutiveSuccesses,
            consecutiveFailures = consecutiveFailures,
        )
    }

    // Must be called under mutex lock
    private fun transitionTo(newState: CircuitState) {
        _state = newState
        when (newState) {
            CircuitState.OPEN -> {
                openedAtMillis = clock()
                halfOpenRequests = 0
                consecutiveSuccesses = 0
            }
            CircuitState.HALF_OPEN -> {
                halfOpenRequests = 0
                consecutiveSuccesses = 0
                consecutiveFailures = 0
            }
            CircuitState.CLOSED -> {
                openedAtMillis = 0L
                halfOpenRequests = 0
                consecutiveFailures = 0
            }
        }
    }
}
