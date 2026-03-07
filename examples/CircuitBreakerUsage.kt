package io.streamline.examples

import io.streamline.sdk.*
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking

/**
 * Demonstrates the CircuitBreaker and RetryPolicy resilience features.
 *
 * The circuit breaker protects against cascading failures by opening the
 * circuit after consecutive failures, rejecting calls during the cooldown
 * period, and then probing with a single request to check recovery.
 *
 * The retry policy adds exponential backoff with jitter for transient
 * errors, integrating with the `isRetryable` flag on all exceptions.
 */
fun main() = runBlocking {

    // ── 1. Configure resilience via StreamlineConfiguration ──────────

    val config = StreamlineConfiguration(
        url = "ws://localhost:9092",
        circuitBreakerConfig = CircuitBreakerConfig(
            failureThreshold = 3,      // Open after 3 consecutive failures
            resetTimeoutMs = 10_000,   // Probe after 10 seconds
            halfOpenMaxAttempts = 1,   // 1 successful probe closes the circuit
        ),
        retryPolicyConfig = RetryPolicyConfig(
            maxRetries = 5,
            baseDelayMs = 200,
            maxDelayMs = 10_000,
            jitter = true,             // Random jitter prevents thundering herd
        ),
    )

    val client = StreamlineClient(config)

    // ── 2. Produce with automatic resilience ─────────────────────────

    try {
        client.connect()

        // Sends are automatically protected by CircuitBreaker + RetryPolicy.
        // If the server is temporarily down, the retry policy will retry with
        // exponential backoff. If failures persist, the circuit breaker opens
        // and throws CircuitBreakerOpenException immediately.
        client.produce("events", key = "user-1", value = """{"action":"click"}""")
        println("Message sent successfully")

    } catch (e: CircuitBreakerOpenException) {
        println("Circuit breaker is OPEN — remaining cooldown: ${e.remainingMs}ms")
        println("Hint: ${e.hint}")
        println("Retryable: ${e.isRetryable}") // true
        println("Error code: ${e.code}")        // CIRCUIT_BREAKER_OPEN
    } catch (e: StreamlineException) {
        println("Error (${e.code}): ${e.message}")
        println("Retryable: ${e.isRetryable}")
        println("Hint: ${e.hint}")
    }

    // ── 3. Monitor circuit breaker state ─────────────────────────────

    println("Circuit state: ${client.circuitBreaker.state}")
    // Possible values: CLOSED, OPEN, HALF_OPEN

    // Manually reset the circuit breaker if needed
    client.circuitBreaker.reset()
    println("Circuit reset to: ${client.circuitBreaker.state}") // CLOSED

    // ── 4. Use RetryPolicy standalone ────────────────────────────────

    val retryPolicy = RetryPolicy(RetryPolicyConfig(
        maxRetries = 3,
        baseDelayMs = 100,
        jitter = true,
        retryIf = { it is ConnectionFailedException },
    ))

    try {
        var attempts = 0
        val result = retryPolicy.execute {
            attempts++
            println("  Attempt $attempts...")
            if (attempts < 3) throw ConnectionFailedException("Server busy")
            "Success on attempt $attempts"
        }
        println("Result: $result")
    } catch (e: Exception) {
        println("All retries exhausted: ${e.message}")
    }

    // ── 5. Use CircuitBreaker standalone ──────────────────────────────

    val breaker = CircuitBreaker(CircuitBreakerConfig(failureThreshold = 2, resetTimeoutMs = 5000))

    // Trip the breaker with failures
    repeat(2) {
        runCatching { breaker.execute<Unit> { throw RuntimeException("Server down") } }
    }
    println("After 2 failures: ${breaker.state}") // OPEN

    // Wait for reset timeout, then probe
    delay(5100)
    try {
        breaker.execute { println("Probe succeeded!") }
        println("Circuit recovered: ${breaker.state}") // CLOSED
    } catch (e: CircuitBreakerOpenException) {
        println("Still open: ${e.remainingMs}ms remaining")
    }

    client.disconnect()
    client.close()
}
