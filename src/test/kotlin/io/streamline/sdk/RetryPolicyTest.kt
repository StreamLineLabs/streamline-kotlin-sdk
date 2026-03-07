package io.streamline.sdk

import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class RetryPolicyTest {

    @Test
    fun `succeeds on first try`() = runTest {
        val policy = RetryPolicy(RetryPolicyConfig(maxRetries = 3))
        val result = policy.execute { 42 }
        assertEquals(42, result)
    }

    @Test
    fun `retries on retryable exception`() = runTest {
        var attempts = 0
        val policy = RetryPolicy(RetryPolicyConfig(maxRetries = 3, baseDelayMs = 1))

        val result = policy.execute {
            attempts++
            if (attempts < 3) throw ConnectionFailedException("transient")
            "ok"
        }

        assertEquals("ok", result)
        assertEquals(3, attempts)
    }

    @Test
    fun `does not retry non-retryable exception`() = runTest {
        var attempts = 0
        val policy = RetryPolicy(RetryPolicyConfig(maxRetries = 3, baseDelayMs = 1))

        assertFailsWith<AuthenticationFailedException> {
            policy.execute {
                attempts++
                throw AuthenticationFailedException("bad creds")
            }
        }

        assertEquals(1, attempts)
    }

    @Test
    fun `exhausts retries and throws last exception`() = runTest {
        var attempts = 0
        val policy = RetryPolicy(RetryPolicyConfig(maxRetries = 2, baseDelayMs = 1))

        assertFailsWith<ConnectionFailedException> {
            policy.execute {
                attempts++
                throw ConnectionFailedException("always fails")
            }
        }

        assertEquals(3, attempts) // initial + 2 retries
    }

    @Test
    fun `custom retryIf predicate`() = runTest {
        var attempts = 0
        val policy = RetryPolicy(RetryPolicyConfig(
            maxRetries = 3,
            baseDelayMs = 1,
            retryIf = { it is IllegalStateException },
        ))

        val result = policy.execute {
            attempts++
            if (attempts < 2) throw IllegalStateException("retry me")
            "ok"
        }

        assertEquals("ok", result)
        assertEquals(2, attempts)
    }

    @Test
    fun `delay is exponential`() {
        val policy = RetryPolicy(RetryPolicyConfig(baseDelayMs = 100, maxDelayMs = 10_000, jitter = false))
        assertEquals(100, policy.computeDelay(0))
        assertEquals(200, policy.computeDelay(1))
        assertEquals(400, policy.computeDelay(2))
        assertEquals(800, policy.computeDelay(3))
    }

    @Test
    fun `delay is capped at maxDelayMs`() {
        val policy = RetryPolicy(RetryPolicyConfig(baseDelayMs = 100, maxDelayMs = 500, jitter = false))
        assertEquals(500, policy.computeDelay(5))
        assertEquals(500, policy.computeDelay(10))
    }

    @Test
    fun `jitter produces delay within expected range`() {
        val policy = RetryPolicy(RetryPolicyConfig(baseDelayMs = 1000, maxDelayMs = 30_000, jitter = true))
        repeat(50) {
            val d = policy.computeDelay(0)
            assertTrue(d in 500..1000, "Jittered delay $d not in [500, 1000]")
        }
    }

    @Test
    fun `zero retries throws immediately`() = runTest {
        var attempts = 0
        val policy = RetryPolicy(RetryPolicyConfig(maxRetries = 0, baseDelayMs = 1))

        assertFailsWith<ConnectionFailedException> {
            policy.execute {
                attempts++
                throw ConnectionFailedException("fail")
            }
        }
        assertEquals(1, attempts)
    }
}
