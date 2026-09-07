/**
 * Circuit breaker example for Streamline Kotlin SDK.
 *
 * The circuit breaker prevents an application from repeatedly attempting
 * operations against a failing server.
 */
package io.streamline.examples

import io.streamline.sdk.CircuitBreaker
import io.streamline.sdk.CircuitBreakerConfig
import io.streamline.sdk.CircuitOpenException
import io.streamline.sdk.CircuitState
import io.streamline.sdk.StreamlineClient
import io.streamline.sdk.StreamlineConfiguration
import kotlinx.coroutines.delay
import kotlin.time.Duration.Companion.seconds

suspend fun main() {
    println("Circuit Breaker Example")
    println("========================================")

    val config =
        StreamlineConfiguration(
            url = System.getenv("STREAMLINE_WS_URL") ?: "ws://localhost:9092",
        )
    val breaker =
        CircuitBreaker(
            CircuitBreakerConfig(
                failureThreshold = 5,
                successThreshold = 2,
                openTimeout = 10.seconds,
                halfOpenMaxRequests = 3,
            ),
        )

    val client = StreamlineClient(config)
    client.connect()
    println("Connected. Circuit state: ${breaker.state()}")

    for (i in 0 until 20) {
        try {
            breaker.allow()
            client.produce("cb-example", value = "message-$i", key = "key-$i")
            breaker.recordSuccess()
            println("  Message $i: sent (circuit: ${breaker.state()})")
        } catch (e: CircuitOpenException) {
            println("  Message $i: circuit breaker is OPEN")
            delay(1000)
        } catch (e: Exception) {
            breaker.recordFailure()
            println("  Message $i: FAILED (${e.message}) (circuit: ${breaker.state()})")
        }
    }

    val counts = breaker.counts()
    println("\nFinal circuit state: ${breaker.state()}")
    println("Successes: ${counts.totalSuccesses}, Failures: ${counts.totalFailures}")

    if (breaker.state() == CircuitState.OPEN) {
        breaker.reset()
        println("Circuit manually reset to: ${breaker.state()}")
    }

    client.disconnect()
    client.close()
    println("Done!")
}
