package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.HttpUrl.Companion.toHttpUrl
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import org.slf4j.LoggerFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.UUID
import java.util.concurrent.TimeUnit

class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
) : PaymentExternalSystemAdapter {

    companion object {
        private val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapterImpl::class.java)
        private val emptyBody = ByteArray(0).toRequestBody()
        private val mapper = ObjectMapper().registerKotlinModule()
    }

    private val accountName = properties.accountName
    private val client = OkHttpClient.Builder().build()
    private val callLimiter = PaymentAccountCallLimiter(properties.rateLimitPerSec, properties.parallelRequests)

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        val transactionId = UUID.randomUUID()
        val submittedAt = now()
        try {
            paymentESService.update(paymentId) {
                it.logSubmission(true, transactionId, submittedAt, Duration.ofMillis(submittedAt - paymentStartedAt))
            }
        } catch (e: Exception) {
            logger.error("[$accountName] Could not record payment submission for payment $paymentId", e)
            throw e
        }

        var interrupted = false
        val result = try {
            callLimiter.executeBefore(deadline) {
                requestPayment(paymentId, transactionId, amount, deadline)
            } ?: ExternalSysResponse(
                transactionId.toString(), paymentId.toString(), false,
                "Payment deadline expired before provider call"
            )
        } catch (e: InterruptedException) {
            interrupted = true
            logger.warn("[$accountName] Payment interrupted for txId: $transactionId, payment: $paymentId", e)
            ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, "Payment interrupted")
        } catch (e: SocketTimeoutException) {
            logger.warn("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
            ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, "Request timeout")
        } catch (e: Exception) {
            logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
            ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
        }

        try {
            paymentESService.update(paymentId) {
                it.logProcessing(result.result, now(), transactionId, reason = result.message)
            }
        } catch (e: Exception) {
            logger.error("[$accountName] Could not record provider result for txId: $transactionId, payment: $paymentId", e)
            throw e
        } finally {
            if (interrupted) Thread.currentThread().interrupt()
        }

        logger.info(
            "[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, " +
                "succeeded: ${result.result}, message: ${result.message}"
        )
    }

    private fun requestPayment(paymentId: UUID, transactionId: UUID, amount: Int, deadline: Long): ExternalSysResponse {
        val url = "http://$paymentProviderHostPort/external/process".toHttpUrl().newBuilder()
            .addQueryParameter("serviceName", properties.serviceName)
            .addQueryParameter("token", token)
            .addQueryParameter("accountName", accountName)
            .addQueryParameter("transactionId", transactionId.toString())
            .addQueryParameter("paymentId", paymentId.toString())
            .addQueryParameter("amount", amount.toString())
            .build()
        val request = Request.Builder()
            .url(url)
            .post(emptyBody)
            .build()

        val remainingMillis = deadline - now()
        if (remainingMillis <= 0) throw SocketTimeoutException("Payment deadline expired before provider call")

        val call = client.newCall(request)
        call.timeout().timeout(remainingMillis, TimeUnit.MILLISECONDS)
        logger.debug("[$accountName] Submit: $paymentId, txId: $transactionId")
        return call.execute().use { response ->
            try {
                val parsed = mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                if (response.isSuccessful) parsed else ExternalSysResponse(
                    transactionId.toString(), paymentId.toString(), false,
                    parsed.message ?: "Provider returned HTTP ${response.code}"
                )
            } catch (e: Exception) {
                logger.error("[$accountName] Invalid provider response for txId: $transactionId, HTTP ${response.code}", e)
                ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
            }
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = accountName
}

fun now() = System.currentTimeMillis()
