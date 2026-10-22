package ru.quipy.payments.logic

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import org.springframework.stereotype.Component

@Component
class PaymentMetrics(registry: MeterRegistry) {
    private val received = registry.counter("payments.received")
    private val providerRequests = registry.counter("payments.provider.requests")
    private val succeeded = registry.counter("payments.completed", "result", "success")
    private val failed = registry.counter("payments.completed", "result", "failed")
    private val expired = registry.counter("payments.expired.before.provider")
    private val providerDuration = registry.timer("payments.provider.duration")

    fun received() = received.increment()

    fun expiredBeforeProvider() = expired.increment()

    fun completed(success: Boolean) {
        if (success) succeeded.increment() else failed.increment()
    }

    fun <T> providerCall(call: () -> T): T {
        providerRequests.increment()
        val sample = Timer.start()
        try {
            return call()
        } finally {
            sample.stop(providerDuration)
        }
    }
}
