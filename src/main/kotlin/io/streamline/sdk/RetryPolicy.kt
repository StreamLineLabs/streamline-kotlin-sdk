package io.streamline.sdk

import kotlinx.coroutines.delay
import kotlin.math.min
import kotlin.random.Random

/**
 * Configuration for the [RetryPolicy].
 *
 * @property maxRetries Maximum number of retry attempts (0 = no retries).
 * @property baseDelayMs Initial delay between retries in milliseconds.
 * @property maxDelayMs Upper bound for the computed delay.
 * @property jitter When true, adds random jitter to avoid thundering-herd effects.
 * @property retryIf Optional predicate — only retry when the exception matches.
 */
data class RetryPolicyConfig(
    val maxRetries: Int = 3,
    val baseDelayMs: Long = 200,
    val maxDelayMs: Long = 30_000,
    val jitter: Boolean = true,
    val retryIf: ((Throwable) -> Boolean)? = null,
)

/**
 * Retry policy with exponential backoff and optional jitter.
 *
 * Wraps a suspending action and retries it up to [RetryPolicyConfig.maxRetries]
 * times using exponential backoff. By default only
 * [retryable][StreamlineException.isRetryable] exceptions trigger a retry;
 * provide a custom [RetryPolicyConfig.retryIf] predicate to override.
 *
 * ```kotlin
 * val policy = RetryPolicy(RetryPolicyConfig(maxRetries = 5))
 * val result = policy.execute { client.produce("topic", value = "data") }
 * ```
 */
class RetryPolicy(private val config: RetryPolicyConfig = RetryPolicyConfig()) {

    /**
     * Execute [action] with retries according to the configured policy.
     *
     * @return The result of a successful invocation.
     * @throws Exception The last exception if all retries are exhausted.
     */
    suspend fun <T> execute(action: suspend () -> T): T {
        var lastException: Exception? = null

        for (attempt in 0..config.maxRetries) {
            try {
                return action()
            } catch (e: Exception) {
                lastException = e

                if (attempt >= config.maxRetries || !shouldRetry(e)) {
                    throw e
                }

                val delayMs = computeDelay(attempt)
                delay(delayMs)
            }
        }

        // Unreachable — the loop always returns or throws.
        throw lastException ?: IllegalStateException("RetryPolicy: unexpected state")
    }

    private fun shouldRetry(e: Throwable): Boolean {
        config.retryIf?.let { return it(e) }
        return (e as? StreamlineException)?.isRetryable ?: false
    }

    internal fun computeDelay(attempt: Int): Long {
        val exponential = config.baseDelayMs * (1L shl attempt.coerceAtMost(30))
        val capped = min(exponential, config.maxDelayMs)
        if (!config.jitter) return capped
        return (capped / 2) + Random.nextLong(capped / 2 + 1)
    }
}
