package ru.quipy.payments.logic

import org.springframework.stereotype.Service
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

@Service
class PaymentSystemImpl(
    private val paymentAccounts: List<PaymentExternalSystemAdapter>
) : PaymentService {
    private val enabledAccounts = paymentAccounts.filter { it.isEnabled() }
    private val nextAccount = AtomicInteger()

    init {
        require(enabledAccounts.isNotEmpty()) { "No payment accounts are enabled" }
    }

    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        val index = Math.floorMod(nextAccount.getAndIncrement(), enabledAccounts.size)
        enabledAccounts[index].performPaymentAsync(paymentId, amount, paymentStartedAt, deadline)
    }
}
