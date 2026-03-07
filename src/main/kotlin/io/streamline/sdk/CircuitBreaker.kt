package io.streamline.sdk

import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.time.Instant

/**
 * State of the circuit breaker.
 *
 * - [CLOSED]: Normal operation — requests pass through and failures are tracked.
 * - [OPEN]: Circuit is tripped — requests are rejected immediately.
 * - [HALF_OPEN]: A single probe request is allowed to determine recovery.
 */
enum class CircuitBreakerState { CLOSED, OPEN, HALF_OPEN }

/**
 * Configuration for a [CircuitBreaker].
 *
 * @property failureThreshold Number of consecutive failures before the circuit opens.
 * @property resetTimeoutMs Time in milliseconds to wait in the OPEN state before probing.
 * @property halfOpenMaxAttempts Number of successful probes required to close the circuit.
 */
data class CircuitBreakerConfig(
    val failureThreshold: Int = 5,
    val resetTimeoutMs: Long = 30_000,
    val halfOpenMaxAttempts: Int = 1,
)

/**
 * Implements the Circuit Breaker resilience pattern.
 *
 * When a configurable number of consecutive failures is reached the breaker
 * transitions from [CircuitBreakerState.CLOSED] to [CircuitBreakerState.OPEN],
 * rejecting all calls immediately. After the reset timeout the breaker moves
 * to [CircuitBreakerState.HALF_OPEN] and allows a probe request. If the probe
 * succeeds the circuit closes; if it fails the circuit re-opens.
 *
 * Thread-safe — all state mutations are protected by a [Mutex].
 *
 * ```kotlin
 * val cb = CircuitBreaker(CircuitBreakerConfig(failureThreshold = 3))
 * val result = cb.execute { riskyRemoteCall() }
 * ```
 */
class CircuitBreaker(private val config: CircuitBreakerConfig = CircuitBreakerConfig()) {

    private val mutex = Mutex()

    private val _stateFlow = MutableStateFlow(CircuitBreakerState.CLOSED)

    /** Observable state flow — collect to react to state transitions. */
    val stateFlow: StateFlow<CircuitBreakerState> = _stateFlow.asStateFlow()

    private var failureCount: Int = 0
    private var halfOpenSuccesses: Int = 0
    private var lastFailureTime: Instant? = null

    // -- Metrics --
    private var _totalFailures: Long = 0
    private var _totalSuccesses: Long = 0
    private var _totalRejections: Long = 0

    /** Total number of failures recorded since creation. */
    val totalFailures: Long get() = _totalFailures

    /** Total number of successes recorded since creation. */
    val totalSuccesses: Long get() = _totalSuccesses

    /** Total number of calls rejected while the circuit was open. */
    val totalRejections: Long get() = _totalRejections

    /** Current state of the circuit breaker (snapshot — may change immediately). */
    val state: CircuitBreakerState get() = _stateFlow.value

    /**
     * Execute [action] through the circuit breaker.
     *
     * @throws CircuitBreakerOpenException when the circuit is open and the
     *   reset timeout has not elapsed.
     */
    suspend fun <T> execute(action: suspend () -> T): T {
        val currentState = acquirePermission()

        return try {
            val result = action()
            onSuccess(currentState)
            result
        } catch (e: Exception) {
            onFailure(currentState)
            throw e
        }
    }

    /** Manually reset the circuit breaker to [CircuitBreakerState.CLOSED]. */
    suspend fun reset() {
        mutex.withLock {
            _stateFlow.value = CircuitBreakerState.CLOSED
            failureCount = 0
            halfOpenSuccesses = 0
            lastFailureTime = null
        }
    }

    private suspend fun acquirePermission(): CircuitBreakerState {
        mutex.withLock {
            return when (_stateFlow.value) {
                CircuitBreakerState.CLOSED -> CircuitBreakerState.CLOSED
                CircuitBreakerState.OPEN -> {
                    val elapsed = lastFailureTime?.let {
                        Instant.now().toEpochMilli() - it.toEpochMilli()
                    } ?: Long.MAX_VALUE

                    if (elapsed >= config.resetTimeoutMs) {
                        _stateFlow.value = CircuitBreakerState.HALF_OPEN
                        halfOpenSuccesses = 0
                        CircuitBreakerState.HALF_OPEN
                    } else {
                        _totalRejections++
                        throw CircuitBreakerOpenException(config.resetTimeoutMs - elapsed)
                    }
                }
                CircuitBreakerState.HALF_OPEN -> CircuitBreakerState.HALF_OPEN
            }
        }
    }

    private suspend fun onSuccess(calledInState: CircuitBreakerState) {
        mutex.withLock {
            _totalSuccesses++
            when (calledInState) {
                CircuitBreakerState.HALF_OPEN -> {
                    halfOpenSuccesses++
                    if (halfOpenSuccesses >= config.halfOpenMaxAttempts) {
                        _stateFlow.value = CircuitBreakerState.CLOSED
                        failureCount = 0
                        halfOpenSuccesses = 0
                        lastFailureTime = null
                    }
                }
                CircuitBreakerState.CLOSED -> {
                    failureCount = 0
                }
                else -> { /* no-op when OPEN — shouldn't reach here */ }
            }
        }
    }

    private suspend fun onFailure(calledInState: CircuitBreakerState) {
        mutex.withLock {
            _totalFailures++
            lastFailureTime = Instant.now()
            when (calledInState) {
                CircuitBreakerState.CLOSED -> {
                    failureCount++
                    if (failureCount >= config.failureThreshold) {
                        _stateFlow.value = CircuitBreakerState.OPEN
                    }
                }
                CircuitBreakerState.HALF_OPEN -> {
                    _stateFlow.value = CircuitBreakerState.OPEN
                    halfOpenSuccesses = 0
                }
                else -> { /* already OPEN */ }
            }
        }
    }
}
