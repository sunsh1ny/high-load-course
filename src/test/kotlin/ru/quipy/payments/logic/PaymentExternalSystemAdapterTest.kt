package ru.quipy.payments.logic

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.time.Duration
import java.util.UUID

class PaymentExternalSystemAdapterTest {
    @Test
    fun `does not call provider when remaining time is reserved for processing`() {
        val registry = SimpleMeterRegistry()
        val eventSourcing = mock(EventSourcingService::class.java) as
                EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
        val adapter = PaymentExternalSystemAdapterImpl(
            PaymentAccountProperties("test", "acc-5", 5, 3, 30, Duration.ofMillis(4900), true),
            eventSourcing,
            "127.0.0.1:1",
            "test-token",
            PaymentMetrics(registry),
            processingReserveMillis = 10_000,
        )
        val startedAt = System.currentTimeMillis()

        adapter.performPaymentAsync(UUID.randomUUID(), 100, startedAt, startedAt + 1000)

        assertEquals(0.0, registry.get("payments.provider.requests").counter().count())
        assertEquals(0L, registry.get("payments.provider.duration").timer().count())
        assertEquals(1.0, registry.get("payments.expired.before.provider").counter().count())
        assertEquals(1.0, registry.get("payments.completed").tag("result", "failed").counter().count())
        registry.close()
    }
}
