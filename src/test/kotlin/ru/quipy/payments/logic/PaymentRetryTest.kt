package ru.quipy.payments.logic

import com.sun.net.httpserver.HttpServer
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.mockito.Mockito.mockingDetails
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.api.PaymentProcessedEvent
import ru.quipy.payments.api.PaymentSubmittedEvent
import java.net.InetSocketAddress
import java.net.URLDecoder
import java.time.Duration
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.Executors

class PaymentRetryTest {
    @Test
    fun `retries an explicit temporary rejection and records only one final result`() {
        Provider(listOf(503 to false, 200 to false, 200 to true)).use { provider ->
            val paymentId = UUID.randomUUID()
            val es = eventStore()
            adapter(provider, es, attempts = 3).performPaymentAsync(paymentId, 100, now(), now() + 5000)

            assertEquals(3, provider.requests.size)
            assertEquals(setOf(paymentId.toString()), provider.requests.map { it.getValue("paymentId") }.toSet())
            assertEquals(3, provider.requests.map { it.getValue("transactionId") }.toSet().size)

            val events = recordedEvents(es, paymentId)
            assertEquals(3, events.filterIsInstance<PaymentSubmittedEvent>().size)
            val result = events.filterIsInstance<PaymentProcessedEvent>().single()
            assertTrue(result.success)
            assertEquals(provider.requests.last().getValue("transactionId"), result.transactionId.toString())
        }
    }

    @Test
    fun `default configuration still makes exactly one attempt`() {
        Provider(listOf(200 to false, 200 to true)).use { provider ->
            val es = eventStore()
            val id = UUID.randomUUID()
            adapter(provider, es).performPaymentAsync(id, 100, now(), now() + 5000)
            assertEquals(1, provider.requests.size)
            assertFalse(recordedEvents(es, id).filterIsInstance<PaymentProcessedEvent>().single().success)
        }
    }

    @Test
    fun `stops immediately after success`() {
        Provider(listOf(200 to true)).use { provider ->
            adapter(provider, eventStore(), attempts = 3).performPaymentAsync(UUID.randomUUID(), 100, now(), now() + 5000)
            assertEquals(1, provider.requests.size)
        }
    }

    @Test
    fun `does not exceed the configured attempt count`() {
        Provider(listOf(200 to false)).use { provider ->
            val es = eventStore()
            val id = UUID.randomUUID()
            adapter(provider, es, attempts = 3).performPaymentAsync(id, 100, now(), now() + 5000)
            assertEquals(3, provider.requests.size)
            assertFalse(recordedEvents(es, id).filterIsInstance<PaymentProcessedEvent>().single().success)
        }
    }

    @Test
    fun `permanent errors and provider limit violations are not retried`() {
        for (code in listOf(400, 401, 403, 404, 429)) {
            Provider(listOf(code to false)).use { provider ->
                adapter(provider, eventStore(), attempts = 3).performPaymentAsync(UUID.randomUUID(), 100, now(), now() + 5000)
                assertEquals(1, provider.requests.size, "HTTP $code must not be retried")
            }
        }
    }

    @Test
    fun `does not retry when the remaining deadline cannot cover processing`() {
        Provider(listOf(200 to false)).use { provider ->
            adapter(provider, eventStore(), attempts = 3, processing = Duration.ofSeconds(1))
                .performPaymentAsync(UUID.randomUUID(), 100, now(), now() + 500)
            assertEquals(1, provider.requests.size)
        }
    }

    @Test
    fun `ambiguous network timeout is not retried`() {
        Provider(listOf(200 to true), delayMillis = 400).use { provider ->
            adapter(provider, eventStore(), attempts = 3).performPaymentAsync(UUID.randomUUID(), 100, now(), now() + 150)
            assertEquals(1, provider.requests.size)
        }
    }

    private fun eventStore(): EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState> =
        mock(EventSourcingService::class.java) { null } as EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    private fun adapter(
        provider: Provider,
        es: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
        attempts: Int = 1,
        processing: Duration = Duration.ofMillis(10),
    ) = PaymentExternalSystemAdapterImpl(
        PaymentAccountProperties("test", "acc-8", 50, 1000, 30, processing, true),
        es, "127.0.0.1:${provider.server.address.port}", "test-token", attempts, 0,
    )

    private fun recordedEvents(
        es: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>, id: UUID,
    ): List<Any> {
        val state = PaymentAggregateState()
        state.paymentCreatedApply(state.create(id, UUID.randomUUID(), 100))
        return mockingDetails(es).invocations.filter { it.method.name == "update" }.map { invocation ->
            val command = invocation.arguments.filterIsInstance<Function1<*, *>>().single() as (PaymentAggregateState) -> Any
            val event = command(state)
            when (event) {
                is PaymentSubmittedEvent -> state.paymentSubmittedApply(event)
                is PaymentProcessedEvent -> state.paymentSubmittedApply(event)
            }
            event
        }
    }

    private class Provider(private val responses: List<Pair<Int, Boolean>>, delayMillis: Long = 0) : AutoCloseable {
        val requests = CopyOnWriteArrayList<Map<String, String>>()
        val server: HttpServer = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
        private val executor = Executors.newCachedThreadPool()

        init {
            server.executor = executor
            server.createContext("/external/process") { exchange ->
                val params = exchange.requestURI.rawQuery.split('&').associate {
                    val parts = it.split('=', limit = 2)
                    parts[0] to URLDecoder.decode(parts[1], Charsets.UTF_8)
                }
                requests.add(params)
                val (code, success) = responses[minOf(requests.size - 1, responses.lastIndex)]
                try {
                    if (delayMillis > 0) Thread.sleep(delayMillis)
                    val body = """{"transactionId":"${params["transactionId"]}","paymentId":"${params["paymentId"]}","result":$success,"message":"test response"}""".toByteArray()
                    exchange.responseHeaders.add("Content-Type", "application/json")
                    exchange.sendResponseHeaders(code, body.size.toLong())
                    exchange.responseBody.use { it.write(body) }
                } catch (_: Exception) {
                    // A timeout test deliberately disconnects before the provider responds.
                } finally {
                    exchange.close()
                }
            }
            server.start()
        }

        override fun close() {
            server.stop(0)
            executor.shutdownNow()
        }
    }
}
