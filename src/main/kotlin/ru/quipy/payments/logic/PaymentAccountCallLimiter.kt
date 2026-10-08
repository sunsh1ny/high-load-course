package ru.quipy.payments.logic

import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit

internal class PaymentAccountCallLimiter(ratePerSecond: Int, parallelRequests: Int) {
    init {
        require(ratePerSecond > 0) { "ratePerSecond must be positive" }
        require(parallelRequests > 0) { "parallelRequests must be positive" }
    }

    private val inFlight = Semaphore(parallelRequests, true)
    private val scheduleLock = Any()
    private val spacingNanos = (1_020_000_000L + ratePerSecond - 1) / ratePerSecond
    private var nextStartNanos = 0L

    fun <T : Any> executeBefore(deadlineMillis: Long, call: () -> T): T? {
        val remainingMillis = deadlineMillis - System.currentTimeMillis()
        if (remainingMillis <= 0 || !inFlight.tryAcquire(remainingMillis, TimeUnit.MILLISECONDS)) {
            return null
        }

        try {
            if (!awaitRateSlot(deadlineMillis)) return null
            return call()
        } finally {
            inFlight.release()
        }
    }

    private fun awaitRateSlot(deadlineMillis: Long): Boolean {
        val waitNanos = synchronized(scheduleLock) {
            val now = System.nanoTime()
            val start = maxOf(now, nextStartNanos)
            val wait = start - now
            val remainingMillis = deadlineMillis - System.currentTimeMillis()
            if (remainingMillis <= 0 || wait >= TimeUnit.MILLISECONDS.toNanos(remainingMillis)) {
                null
            } else {
                nextStartNanos = start + spacingNanos
                wait
            }
        } ?: return false

        if (waitNanos > 0) TimeUnit.NANOSECONDS.sleep(waitNanos)
        return System.currentTimeMillis() < deadlineMillis
    }
}
