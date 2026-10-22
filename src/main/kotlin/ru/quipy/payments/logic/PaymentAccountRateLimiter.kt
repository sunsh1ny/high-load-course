package ru.quipy.payments.logic

import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock

internal class PaymentAccountRateLimiter(ratePerSecond: Int) {
    init {
        require(ratePerSecond > 0) { "ratePerSecond must be positive" }
    }

    private val scheduleLock = ReentrantLock(true)
    // Leave room for scheduling and transport jitter before requests reach the provider.
    private val spacingNanos = (1_100_000_000L + ratePerSecond - 1) / ratePerSecond
    private var nextStartNanos = 0L

    fun awaitPermit(deadlineMillis: Long): Boolean {
        val remainingMillis = deadlineMillis - System.currentTimeMillis()
        if (remainingMillis <= 0 || !scheduleLock.tryLock(remainingMillis, TimeUnit.MILLISECONDS)) return false
        try {
            while (true) {
                val remaining = deadlineMillis - System.currentTimeMillis()
                if (remaining <= 0) return false
                val wait = nextStartNanos - System.nanoTime()
                if (wait <= 0) {
                    // Pace actual grants: reserved timestamps can bunch up after a scheduler pause.
                    nextStartNanos = System.nanoTime() + spacingNanos
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
