package ru.quipy.payments.logic

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.util.concurrent.Callable
import java.util.concurrent.Executors

class PaymentAdmissionLimiterTest {
    private var nowMillis = 0L
    private fun limiter() = PendingPaymentLimiter(11, 1200, { nowMillis })

    @Test
    fun `elapsed time cannot release capacity while accepted payments are unfinished`() {
        val limiter = limiter()
        val permits = (1..15).map { limiter.acquire(2500) }
        assertThrows(PaymentAdmissionRejected::class.java) { limiter.acquire(2500) }
        nowMillis = 1000
        assertThrows(PaymentAdmissionRejected::class.java) { limiter.acquire(nowMillis + 2500) }

        permits.first().close()
        limiter.acquire(nowMillis + 2500).close()
        permits.forEach { it.close() }
        assertEquals(0, limiter.pendingCount())
    }

    @Test
    fun `a long deadline admits the entire 270 purchase burst`() {
        val limiter = limiter()
        val first = (1..270).map { limiter.acquire(26000) }
        first.forEach { it.close() }
        nowMillis = 90_000
        val second = (1..270).map { limiter.acquire(nowMillis + 26000) }
        second.forEach { it.close() }
        assertEquals(0, limiter.pendingCount())
    }

    @Test
    fun `failed enqueue releases its permit and closing twice is harmless`() {
        val limiter = limiter()
        repeat(30) {
            val permit = limiter.acquire(2500)
            try {
                throw PaymentBufferFull()
            } catch (_: PaymentBufferFull) {
                permit.close()
            }
            permit.close()
        }
        assertEquals(0, limiter.pendingCount())
    }

    @Test
    fun `insufficient processing budget rejects admission`() {
        val limiter = limiter()
        assertThrows(PaymentAdmissionRejected::class.java) { limiter.acquire(1000) }
        assertEquals(0, limiter.pendingCount())
    }

    @Test
    fun `concurrent admission cannot exceed the pending payment bound`() {
        val limiter = limiter()
        val pool = Executors.newFixedThreadPool(8)
        try {
            val permits = pool.invokeAll((1..40).map {
                Callable { try { limiter.acquire(2500) } catch (_: PaymentAdmissionRejected) { null } }
            }).mapNotNull { it.get() }
            assertEquals(15, permits.size)
            permits.forEach { it.close() }
            assertEquals(0, limiter.pendingCount())
        } finally {
            pool.shutdownNow()
        }
    }
}
