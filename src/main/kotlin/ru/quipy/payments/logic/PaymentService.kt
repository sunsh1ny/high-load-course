package ru.quipy.payments.logic

import java.time.Duration
import java.util.*

interface PaymentService {
    fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long)
}

interface PaymentExternalSystemAdapter {
    fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long)

    fun name(): String

    fun price(): Int

    fun isEnabled(): Boolean
}

data class PaymentAccountProperties(
    val serviceName: String,
    val accountName: String,
    val parallelRequests: Int,
    val rateLimitPerSec: Int,
    val price: Int,
    val averageProcessingTime: Duration = Duration.ofSeconds(11),
    val enabled: Boolean,
)

class ExternalSysResponse(
    val transactionId: String,
    val paymentId: String,
    val result: Boolean,
    val message: String? = null,
)
