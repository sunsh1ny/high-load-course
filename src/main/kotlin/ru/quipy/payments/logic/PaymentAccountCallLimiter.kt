package ru.quipy.payments.logic

import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit

internal class PaymentAccountCallLimiter(ratePerSecond: Int, parallelRequests: Int) {
    private val rateLimiter = PaymentAccountRateLimiter(ratePerSecond)

    init {
        require(parallelRequests > 0) { "parallelRequests must be positive" }
    }

    private val inFlight = Semaphore(parallelRequests, true)

    fun <T : Any> executeBefore(deadlineMillis: Long, call: () -> T): T? {
        val remainingMillis = deadlineMillis - System.currentTimeMillis()
        if (remainingMillis <= 0 || !inFlight.tryAcquire(remainingMillis, TimeUnit.MILLISECONDS)) {
            return null
        }

        try {
            if (!rateLimiter.awaitPermit(deadlineMillis)) return null
            return call()
        } finally {
            inFlight.release()
        }
    }
}
