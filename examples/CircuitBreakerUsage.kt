/**
 * Circuit breaker example for Streamline Kotlin SDK.
 *
 * Prerequisites:
 *   1. Start a Streamline server:  streamline --playground
 *   2. Run this example:           gradle run (or copy into your project)
 *
 * The circuit breaker prevents your application from repeatedly attempting
 * operations against a failing server. After consecutive failures it "opens"
 * and rejects requests immediately, giving the server time to recover.
 */
package io.streamline.examples

import io.streamline.sdk.*
import kotlinx.coroutines.*
import kotlin.time.Duration.Companion.seconds

suspend fun main() {
    println("Circuit Breaker Example")
    println("========================================")

    val config = StreamlineConfig(
        url = System.getenv("STREAMLINE_WS_URL") ?: "ws://localhost:9092",
    )

    // Configure the circuit breaker
    val cb = CircuitBreaker(
        CircuitBreakerConfig(
            failureThreshold = 5,       // Open after 5 consecutive failures
            successThreshold = 2,       // Close after 2 successes in half-open
            openTimeout = 10.seconds,   // Wait 10s before probing
            halfOpenMaxRequests = 3,    // Allow 3 probe requests in half-open
            onStateChange = { from, to ->
                println("  [Circuit Breaker] $from → $to")
            },
        )
    )

    val client = StreamlineClient(config)
    client.connect()
    println("Connected. Circuit state: ${cb.state}")

    // Send messages through the circuit breaker
    for (i in 0 until 20) {
        if (!cb.allow()) {
            println("  Message $i: REJECTED (circuit open)")
            delay(1000)
            continue
        }

        try {
            client.produce("cb-example", value = "message-$i", key = "key-$i")
            cb.recordSuccess()
            println("  Message $i: sent (circuit: ${cb.state})")
        } catch (e: CircuitBreakerOpenException) {
            println("  Message $i: circuit breaker is OPEN")
        } catch (e: Exception) {
            cb.recordFailure()
            println("  Message $i: FAILED (${e.message}) (circuit: ${cb.state})")
        }
    }

    // Show final state
    val counts = cb.counts
    println("\nFinal circuit state: ${cb.state}")
    println("Successes: ${counts.successes}, Failures: ${counts.failures}")

    // Manual reset
    if (cb.state == CircuitState.OPEN) {
        cb.reset()
        println("Circuit manually reset to: ${cb.state}")
    }

    client.disconnect()
    println("Done!")
}
