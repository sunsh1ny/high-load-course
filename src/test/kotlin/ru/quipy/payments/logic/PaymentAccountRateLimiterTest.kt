package ru.quipy.payments.logic

import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class PaymentAccountRateLimiterTest {
    @Test
    fun `rejects non-positive rates`() {
        for (rate in listOf(0, -1)) {
            assertThrows(IllegalArgumentException::class.java) {
                PaymentAccountRateLimiter(rate)
            }
        }
    }

    @Test
    fun `an expired deadline does not reserve a rate slot`() {
        val limiter = PaymentAccountRateLimiter(ratePerSecond = 1)

        assertFalse(limiter.awaitPermit(System.currentTimeMillis() - 1))
        assertTrue(limiter.awaitPermit(System.currentTimeMillis() + 1_000))
    }

    @Test
    fun `a slot that misses the deadline is not reserved`() {
        val limiter = PaymentAccountRateLimiter(ratePerSecond = 1)
        assertTrue(limiter.awaitPermit(System.currentTimeMillis() + 5_000))

        assertFalse(limiter.awaitPermit(System.currentTimeMillis() + 50))
        assertTrue(limiter.awaitPermit(System.currentTimeMillis() + 1_500))
    }
}
