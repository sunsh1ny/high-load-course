package ru.quipy.payments.logic

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Gauge
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

/** Bound the admitted backlog by the time left to finish a payment. */
@Component
class PaymentAdmissionLimiter(
    @Value("\${payment.admission.enabled:false}") private val enabled: Boolean,
    @Value("\${payment.admission.rate-per-second:11}") ratePerSecond: Int,
    @Value("\${payment.admission.processing-reserve-millis:1200}") reserveMillis: Long,
    registry: MeterRegistry,
) {
    private val limiter = PendingPaymentLimiter(ratePerSecond, reserveMillis)
    private val accepted = registry.counter("payment.admission.accepted")
    private val rejected = registry.counter("payment.admission.rejected")

    init {
        Gauge.builder("payment.admission.pending", limiter) { it.pendingCount().toDouble() }.register(registry)
    }

    fun acquire(deadlineMillis: Long): AutoCloseable {
        if (!enabled) return AutoCloseable {}
        return try {
            val permit = limiter.acquire(deadlineMillis)
            accepted.increment()
            permit
        } catch (e: PaymentAdmissionRejected) {
            rejected.increment()
            throw e
        }
    }
}

/** A bounded bucket that drains on actual completion, not on estimated elapsed time. */
internal class PendingPaymentLimiter(
    ratePerSecond: Int,
    private val reserveMillis: Long,
    private val millis: () -> Long = System::currentTimeMillis,
) {
    init {
        require(ratePerSecond > 0)
        require(reserveMillis >= 0)
    }

    private val spacingNanos = (PAYMENT_RATE_PERIOD_NANOS + ratePerSecond - 1) / ratePerSecond
    private var pending = 0

    @Synchronized
    fun acquire(deadlineMillis: Long): AutoCloseable {
        val nowMillis = millis()
        val budgetMillis = deadlineMillis - nowMillis - reserveMillis
        val capacity = if (budgetMillis < 0) 0 else
            TimeUnit.MILLISECONDS.toNanos(budgetMillis) / spacingNanos + 1
        if (pending >= capacity) {
            throw PaymentAdmissionRejected(nowMillis + (spacingNanos + 999_999) / 1_000_000)
        }
        pending++
        val released = AtomicBoolean()
        return AutoCloseable {
            if (released.compareAndSet(false, true)) {
                synchronized(this) { pending-- }
            }
        }
    }

    @Synchronized
    fun pendingCount() = pending
}

class PaymentAdmissionRejected(val retryAtMillis: Long) : RuntimeException("Payment admission limit exceeded")
