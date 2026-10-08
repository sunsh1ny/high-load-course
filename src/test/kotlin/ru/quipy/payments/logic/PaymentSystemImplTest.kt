package ru.quipy.payments.logic

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

class PaymentSystemImplTest {
    @Test
    fun `submits each payment through exactly one enabled account`() {
        val first = CountingAccount(true)
        val second = CountingAccount(true)
        val disabled = CountingAccount(false)
        val paymentSystem = PaymentSystemImpl(listOf(first, disabled, second))

        repeat(20) {
            paymentSystem.submitPaymentRequest(UUID.randomUUID(), 100, 0, Long.MAX_VALUE)
        }

        assertEquals(10, first.calls.get())
        assertEquals(10, second.calls.get())
        assertEquals(0, disabled.calls.get())
    }

    @Test
    fun `fails at startup when no account is enabled`() {
        assertThrows(IllegalArgumentException::class.java) {
            PaymentSystemImpl(listOf(CountingAccount(false)))
        }
    }

    private class CountingAccount(private val enabled: Boolean) : PaymentExternalSystemAdapter {
        val calls = AtomicInteger()

        override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
            calls.incrementAndGet()
        }

        override fun name() = "test"

        override fun price() = 30

        override fun isEnabled() = enabled
    }
}
