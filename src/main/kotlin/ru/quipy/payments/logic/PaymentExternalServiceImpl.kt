package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import okhttp3.Call
import okhttp3.EventListener
import okhttp3.HttpUrl.Companion.toHttpUrl
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import okhttp3.RequestBody.Companion.toRequestBody
import okio.BufferedSink
import org.slf4j.LoggerFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.InterruptedIOException
import java.net.HttpURLConnection.HTTP_CLIENT_TIMEOUT
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
    private val retryServerTimeout: Boolean = false,
    registry: MeterRegistry? = null,
) : PaymentExternalSystemAdapter {

    init {
        require(maxAttempts > 0) { "maxAttempts must be positive" }
        require(retryDelayMillis >= 0) { "retryDelayMillis must not be negative" }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapterImpl::class.java)
        private val emptyBody = ByteArray(0).toRequestBody()
        private val singleSendBody = object : RequestBody() {
            override fun contentType() = null
            override fun contentLength() = 0L
            override fun writeTo(sink: BufferedSink) = Unit
            override fun isOneShot() = true
        }
        private val mapper = ObjectMapper().registerKotlinModule()
        private val timeoutResponseReader = mapper.readerFor(JsonNode::class.java)
            .with(DeserializationFeature.FAIL_ON_TRAILING_TOKENS, DeserializationFeature.FAIL_ON_READING_DUP_TREE_KEY)
        private const val SERVER_TIMEOUT_ATTEMPTS = 3
        private const val MAX_PROVIDER_TIMEOUT_MILLIS = 1800L
        private const val MIN_PROVIDER_TIMEOUT_MILLIS = 1300L
        private const val RESPONSE_MARGIN_MILLIS = 250L
        private const val MIN_ATTEMPT_SLOT_MILLIS = MIN_PROVIDER_TIMEOUT_MILLIS + RESPONSE_MARGIN_MILLIS
        private const val RETRY_LIMITER_RESERVE_MILLIS = 400L
        private const val CASE7_RATE_PERIOD_NANOS = 1_080_000_000L
    }

    private val accountName = properties.accountName
    private val case7Registry = registry.takeIf { retryServerTimeout }
    private val retries = case7Registry?.counter("payment.provider.retries", "account", accountName)
    private val serverTimeouts = case7Registry?.counter("payment.provider.server.timeouts", "account", accountName)
    private val attemptDuration = case7Registry?.let {
        Timer.builder("payment.provider.attempt.duration")
            .tag("account", accountName)
            .publishPercentileHistogram()
            .register(it)
    }
    private val client = OkHttpClient.Builder().retryOnConnectionFailure(false).apply {
        if (retryServerTimeout) {
            followRedirects(false)
            eventListener(object : EventListener() {
                override fun requestHeadersEnd(call: Call, request: Request) {
                    val attempt = call.request().tag(ProviderAttempt::class.java) ?: return
                    attempt.sentAt = now()
                    if (attempt.number > 1) retries?.increment()
                }
            })
        }
    }.build()
    private val callLimiter = PaymentAccountCallLimiter(
        properties.rateLimitPerSec,
        properties.parallelRequests,
        if (retryServerTimeout) CASE7_RATE_PERIOD_NANOS else PAYMENT_RATE_PERIOD_NANOS,
    )

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        var transactionId: UUID? = null
        var result = AttemptResult(false, "Payment deadline expired")
        var interrupted = false
        val attemptLimit = if (retryServerTimeout) SERVER_TIMEOUT_ATTEMPTS else maxAttempts
        val latestPermit = if (retryServerTimeout) deadline - MIN_ATTEMPT_SLOT_MILLIS else deadline

        for (attempt in 1..attemptLimit) {
            if (attempt > 1 && !retryServerTimeout) {
                val pause = retryDelayMillis * (attempt - 1)
                val processingBudget = properties.averageProcessingTime.toMillis() + 100
                if (deadline - now() <= pause + processingBudget) break
                try {
                    TimeUnit.MILLISECONDS.sleep(pause)
                } catch (e: InterruptedException) {
                    interrupted = true
                    break
                }
            }

            val attemptId = UUID.randomUUID()
            val providerAttempt = if (retryServerTimeout) ProviderAttempt(attempt) else null
            if (providerAttempt == null) {
                recordSubmission(paymentId, attemptId, paymentStartedAt, now())
                transactionId = attemptId
            }

            result = try {
                callLimiter.executeBefore(latestPermit) {
                    requestPayment(paymentId, attemptId, amount, deadline, providerAttempt)
                } ?: AttemptResult(false, "Payment deadline expired before provider call")
            } catch (e: InterruptedException) {
                interrupted = true
                AttemptResult(false, "Payment interrupted")
            } catch (e: InterruptedIOException) {
                logger.warn("[$accountName] Payment timeout for txId: $attemptId, payment: $paymentId", e)
                AttemptResult(false, "Request timeout")
            } catch (e: Exception) {
                logger.error("[$accountName] Payment failed for txId: $attemptId, payment: $paymentId", e)
                AttemptResult(false, e.message)
            }

            providerAttempt?.sentAt?.let { submittedAt ->
                recordSubmission(paymentId, attemptId, paymentStartedAt, submittedAt)
                transactionId = attemptId
            }
            if (!result.retryable) break
            logger.debug("[$accountName] Retryable provider response on attempt $attempt for payment $paymentId")
        }

        val finalResult = result
        val finalTransactionId = transactionId
        try {
            paymentESService.update(paymentId) {
                it.logProcessing(finalResult.success, now(), finalTransactionId, reason = finalResult.message)
            }
        } catch (e: Exception) {
            logger.error("[$accountName] Could not record provider result for txId: $transactionId, payment: $paymentId", e)
            throw e
        } finally {
            if (interrupted) Thread.currentThread().interrupt()
        }

        logger.info(
            "[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, " +
                "succeeded: ${result.success}, message: ${result.message}"
        )
    }

    private fun recordSubmission(paymentId: UUID, transactionId: UUID, paymentStartedAt: Long, submittedAt: Long) {
        paymentESService.update(paymentId) {
            it.logSubmission(true, transactionId, submittedAt, Duration.ofMillis(submittedAt - paymentStartedAt))
        }
    }

    private data class AttemptResult(val success: Boolean, val message: String?, val retryable: Boolean = false)
    private class ProviderAttempt(val number: Int) {
        var sentAt: Long? = null
    }

    private fun requestPayment(
        paymentId: UUID,
        transactionId: UUID,
        amount: Int,
        deadline: Long,
        attempt: ProviderAttempt?,
    ): AttemptResult {
        val url = "http://$paymentProviderHostPort/external/process".toHttpUrl().newBuilder()
            .addQueryParameter("serviceName", properties.serviceName)
            .addQueryParameter("token", token)
            .addQueryParameter("accountName", accountName)
            .addQueryParameter("transactionId", transactionId.toString())
            .addQueryParameter("paymentId", paymentId.toString())
            .addQueryParameter("amount", amount.toString())

        if (attempt != null) {
            val processingBudget = deadline - now() - RESPONSE_MARGIN_MILLIS
            if (processingBudget < MIN_PROVIDER_TIMEOUT_MILLIS) {
                return AttemptResult(false, "Payment deadline expired before provider call")
            }
            val remainingRetries = SERVER_TIMEOUT_ATTEMPTS - attempt.number
            val affordableRetries = minOf(
                remainingRetries.toLong(),
                (processingBudget - MIN_PROVIDER_TIMEOUT_MILLIS) / MIN_ATTEMPT_SLOT_MILLIS,
            )
            val retryReserve = affordableRetries * (MIN_ATTEMPT_SLOT_MILLIS + RETRY_LIMITER_RESERVE_MILLIS)
            val timeoutMillis = (processingBudget - retryReserve)
                .coerceIn(MIN_PROVIDER_TIMEOUT_MILLIS, MAX_PROVIDER_TIMEOUT_MILLIS)
            val serverTimeout = Duration.ofMillis(timeoutMillis)
            url.addQueryParameter("timeout", serverTimeout.toString())
        }

        val request = Request.Builder()
            .url(url.build())
            .post(if (retryServerTimeout) singleSendBody else emptyBody)
            .tag(ProviderAttempt::class.java, attempt)
            .build()

        val remainingMillis = deadline - now()
        if (remainingMillis <= 0) return AttemptResult(false, "Payment deadline expired before provider call")

        val call = client.newCall(request)
        call.timeout().timeout(remainingMillis, TimeUnit.MILLISECONDS)
        logger.debug("[$accountName] Submit: $paymentId, txId: $transactionId")
        val startedAtNanos = System.nanoTime()
        try {
            return call.execute().use { response ->
                if (response.code == HTTP_CLIENT_TIMEOUT) serverTimeouts?.increment()
                val body = response.body?.string()
                val httpFailure = "Provider returned HTTP ${response.code}"
                try {
                    if (retryServerTimeout && response.code == HTTP_CLIENT_TIMEOUT) {
                        val parsed: JsonNode = timeoutResponseReader.readValue(body)
                        val message = parsed.path("message").textValue()
                        val result = parsed.path("result")
                        val confirmedTimeout = result.isBoolean && !result.booleanValue() && message == "Timeout" &&
                            parsed.path("transactionId").textValue() == transactionId.toString() &&
                            parsed.path("paymentId").textValue() == paymentId.toString()
                        AttemptResult(false, message ?: httpFailure, confirmedTimeout)
                    } else {
                        val parsed = mapper.readValue(body, ExternalSysResponse::class.java)
                        val retryable = !retryServerTimeout && !parsed.result &&
                            (response.isSuccessful || response.code in setOf(500, 502, 503, 504))
                        val message = if (response.isSuccessful) parsed.message else parsed.message ?: httpFailure
                        AttemptResult(response.isSuccessful && parsed.result, message, retryable)
                    }
                } catch (e: Exception) {
                    logger.error("[$accountName] Invalid provider response for txId: $transactionId, HTTP ${response.code}", e)
                    AttemptResult(false, e.message)
                }
            }
        } finally {
            attemptDuration?.record(System.nanoTime() - startedAtNanos, TimeUnit.NANOSECONDS)
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = accountName
}

fun now() = System.currentTimeMillis()
