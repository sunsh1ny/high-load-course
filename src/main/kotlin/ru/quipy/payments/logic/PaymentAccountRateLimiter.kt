package ru.quipy.payments.logic

import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock

// Use the same service rate for provider pacing and admission estimates.
internal const val PAYMENT_RATE_PERIOD_NANOS = 1_010_000_000L

internal class PaymentAccountRateLimiter(ratePerSecond: Int) {
    init {
        require(ratePerSecond > 0) { "ratePerSecond must be positive" }
    }

    private val scheduleLock = ReentrantLock(true)
    // Spread calls evenly and leave 1% for transport/scheduling jitter.
    private val spacingNanos = (PAYMENT_RATE_PERIOD_NANOS + ratePerSecond - 1) / ratePerSecond
    private var nextStartNanos = 0L

    fun awaitPermit(deadlineMillis: Long): Boolean {
        val remainingMillis = deadlineMillis - System.currentTimeMillis()
        if (remainingMillis <= 0 || !scheduleLock.tryLock(remainingMillis, TimeUnit.MILLISECONDS)) return false
        try {
            while (true) {
                val remaining = deadlineMillis - System.currentTimeMillis()
                if (remaining <= 0) return false
                val now = System.nanoTime()
                val wait = nextStartNanos - now
                if (wait <= 0) {
                    // Pace actual grants; never catch up with a burst after a pause.
                    nextStartNanos = now + spacingNanos
                    return true
                }
                if (wait >= TimeUnit.MILLISECONDS.toNanos(remaining)) return false
                TimeUnit.NANOSECONDS.sleep(wait)
            }
        } finally {
            scheduleLock.unlock()
        }
    }
}
