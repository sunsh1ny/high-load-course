package ru.quipy.apigateway

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.*
import org.mockito.Mockito.*
import org.springframework.test.util.ReflectionTestUtils
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post
import org.springframework.test.web.servlet.setup.MockMvcBuilders
import ru.quipy.orders.repository.OrderRepository
import ru.quipy.payments.logic.*
import java.util.UUID

class PaymentBackpressureTest {
    @Test
    fun `marking an accepted payment in progress preserves a concurrent paid result`() {
        val repository = OrderRepository()
        val order = APIController.Order(UUID.randomUUID(), UUID.randomUUID(), 0, APIController.OrderStatus.COLLECTING, 100)
        repository.save(order)
        repository.markPaymentInProgress(order.id)
        assertEquals(APIController.OrderStatus.PAYMENT_IN_PROGRESS, repository.findById(order.id)!!.status)
        repository.save(order.copy(status = APIController.OrderStatus.PAID))
        repository.markPaymentInProgress(order.id)
        assertEquals(APIController.OrderStatus.PAID, repository.findById(order.id)!!.status)
    }

    @Test
    fun `rejected payment preserves order and maps overload deadline and shutdown to HTTP`() {
        val repository = OrderRepository()
        val payer = mock(OrderPayer::class.java)
        val controller = APIController()
        ReflectionTestUtils.setField(controller, "orderRepository", repository)
        ReflectionTestUtils.setField(controller, "orderPayer", payer)
        val mvc = MockMvcBuilders.standaloneSetup(controller).build()
        val order = APIController.Order(UUID.randomUUID(), UUID.randomUUID(), 0, APIController.OrderStatus.COLLECTING, 100)
        repository.save(order)
        for ((failure, status) in listOf(PaymentBufferFull() to 429, PaymentDeadlineExceeded() to 408, PaymentBufferUnavailable() to 503)) {
            `when`(payer.processPayment(eq(order.id) ?: order.id, eq(100), any(UUID::class.java) ?: order.id, anyLong())).thenThrow(failure)
            val response = mvc.perform(post("/orders/${order.id}/payment").param("deadline", Long.MAX_VALUE.toString())).andReturn().response
            assertEquals(status, response.status)
            assertEquals(APIController.OrderStatus.COLLECTING, repository.findById(order.id)!!.status)
            reset(payer)
        }
    }
}
