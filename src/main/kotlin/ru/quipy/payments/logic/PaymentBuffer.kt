package ru.quipy.payments.logic

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Gauge
import jakarta.annotation.PreDestroy
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import ru.quipy.common.utils.NamedThreadFactory
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

@Component
class PaymentBuffer(
    @Value("\${payment.buffer.workers:16}") workers: Int,
    @Value("\${payment.buffer.capacity:256}") capacity: Int,
    registry: MeterRegistry,
) {
    private val logger = LoggerFactory.getLogger(javaClass)
    private val executor = ThreadPoolExecutor(
        workers, workers, 0L, TimeUnit.MILLISECONDS,
        ArrayBlockingQueue(capacity), NamedThreadFactory("payment-submission-executor"),
        ThreadPoolExecutor.AbortPolicy(),
    )
    private val rejected = registry.counter("payment.buffer.rejected", "reason", "full")
    private val expired = registry.counter("payment.buffer.tasks", "outcome", "expired")
    private val failed = registry.counter("payment.buffer.tasks", "outcome", "failed")
    private val completed = registry.counter("payment.buffer.tasks", "outcome", "completed")

    init {
        Gauge.builder("payment.buffer.queued", executor) { it.queue.size.toDouble() }.register(registry)
        Gauge.builder("payment.buffer.active", executor) { it.activeCount.toDouble() }.register(registry)
        Gauge.builder("payment.buffer.capacity", capacity) { it.toDouble() }.strongReference(true).register(registry)
    }

    fun submit(deadline: Long, task: () -> Unit) {
        if (deadline <= System.currentTimeMillis()) throw PaymentDeadlineExceeded()
        try {
            executor.execute {
                if (deadline <= System.currentTimeMillis()) {
                    expired.increment()
                } else {
                    try {
                        task()
                        completed.increment()
                    } catch (e: Exception) {
                        failed.increment()
                        logger.error("Buffered payment failed", e)
                    }
                }
            }
        } catch (e: RejectedExecutionException) {
            if (executor.isShutdown) throw PaymentBufferUnavailable()
            rejected.increment()
            throw PaymentBufferFull()
        }
    }

    @PreDestroy
    fun close() {
        executor.shutdown()
        try {
            if (!executor.awaitTermination(30, TimeUnit.SECONDS)) executor.shutdownNow()
        } catch (e: InterruptedException) {
            executor.shutdownNow()
            Thread.currentThread().interrupt()
        }
    }
}

class PaymentBufferFull : RuntimeException("Payment buffer is full")
class PaymentBufferUnavailable : RuntimeException("Payment buffer is shutting down")
class PaymentDeadlineExceeded : RuntimeException("Payment deadline has expired")
