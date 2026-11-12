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
    private val maxAttempts: Int = 1,
    private val retryDelayMillis: Long = 50,
) : PaymentExternalSystemAdapter {

    init {
        require(maxAttempts > 0) { "maxAttempts must be positive" }
        require(retryDelayMillis >= 0) { "retryDelayMillis must not be negative" }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapterImpl::class.java)
        private val emptyBody = ByteArray(0).toRequestBody()
        private val mapper = ObjectMapper().registerKotlinModule()
    }

    private val accountName = properties.accountName
    private val client = OkHttpClient.Builder().retryOnConnectionFailure(false).build()
    private val callLimiter = PaymentAccountCallLimiter(properties.rateLimitPerSec, properties.parallelRequests)

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        var transactionId = UUID.randomUUID()
        var result = ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, "Payment deadline expired")
        var interrupted = false

        for (attempt in 1..maxAttempts) {
            if (attempt > 1) {
                val pause = retryDelayMillis * (attempt - 1)
                val processingBudget = properties.averageProcessingTime.toMillis() + 100
                if (deadline - now() <= pause + processingBudget) break
                try {
                    TimeUnit.MILLISECONDS.sleep(pause)
                } catch (e: InterruptedException) {
                    interrupted = true
                    break
                }
                transactionId = UUID.randomUUID()
            }

            val attemptId = transactionId
            val submittedAt = now()
            paymentESService.update(paymentId) {
                it.logSubmission(true, attemptId, submittedAt, Duration.ofMillis(submittedAt - paymentStartedAt))
            }

            val outcome = try {
                callLimiter.executeBefore(deadline) {
                    requestPayment(paymentId, attemptId, amount, deadline)
                } ?: AttemptResult(ExternalSysResponse(
                    attemptId.toString(), paymentId.toString(), false,
                    "Payment deadline expired before provider call"
                ))
            } catch (e: InterruptedException) {
                interrupted = true
                AttemptResult(ExternalSysResponse(attemptId.toString(), paymentId.toString(), false, "Payment interrupted"))
            } catch (e: SocketTimeoutException) {
                logger.warn("[$accountName] Payment timeout for txId: $attemptId, payment: $paymentId", e)
                AttemptResult(ExternalSysResponse(attemptId.toString(), paymentId.toString(), false, "Request timeout"))
            } catch (e: Exception) {
                logger.error("[$accountName] Payment failed for txId: $attemptId, payment: $paymentId", e)
                AttemptResult(ExternalSysResponse(attemptId.toString(), paymentId.toString(), false, e.message))
            }
            result = outcome.response
            if (result.result || !outcome.retryable || interrupted) break
            logger.debug("[$accountName] Explicit provider failure on attempt $attempt for payment $paymentId")
        }

        val finalResult = result
        val finalTransactionId = transactionId
        try {
            paymentESService.update(paymentId) {
                it.logProcessing(finalResult.result, now(), finalTransactionId, reason = finalResult.message)
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

    private data class AttemptResult(val response: ExternalSysResponse, val retryable: Boolean = false)

    private fun requestPayment(paymentId: UUID, transactionId: UUID, amount: Int, deadline: Long): AttemptResult {
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
                val result = if (response.isSuccessful) parsed else ExternalSysResponse(
                    transactionId.toString(), paymentId.toString(), false,
                    parsed.message ?: "Provider returned HTTP ${response.code}"
                )
                // Retry only explicit failures. Timeouts, invalid responses, auth and limit errors are not retried.
                AttemptResult(result, !parsed.result && (response.isSuccessful || response.code in setOf(500, 502, 503, 504)))
            } catch (e: Exception) {
                logger.error("[$accountName] Invalid provider response for txId: $transactionId, HTTP ${response.code}", e)
                AttemptResult(ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message))
            }
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = accountName
}

fun now() = System.currentTimeMillis()
