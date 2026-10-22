package ru.quipy.apigateway

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import ru.quipy.orders.repository.OrderRepository
import ru.quipy.payments.logic.OrderPayer
import ru.quipy.payments.logic.PaymentBufferFull
import ru.quipy.payments.logic.PaymentBufferUnavailable
import ru.quipy.payments.logic.PaymentDeadlineExceeded
import java.util.*

@RestController
class APIController {

    val logger: Logger = LoggerFactory.getLogger(APIController::class.java)

    @Autowired
    private lateinit var orderRepository: OrderRepository

    @Autowired
    private lateinit var orderPayer: OrderPayer

    @PostMapping("/users")
    fun createUser(@RequestBody req: CreateUserRequest): User {
        return User(UUID.randomUUID(), req.name)
    }

    data class CreateUserRequest(val name: String, val password: String)

    data class User(val id: UUID, val name: String)

    @PostMapping("/orders")
    fun createOrder(@RequestParam userId: UUID, @RequestParam price: Int): Order {
        val order = Order(
            UUID.randomUUID(),
            userId,
            System.currentTimeMillis(),
            OrderStatus.COLLECTING,
            price,
        )
        return orderRepository.save(order)
    }

    data class Order(
        val id: UUID,
        val userId: UUID,
        val timeCreated: Long,
        val status: OrderStatus,
        val price: Int,
    )

    enum class OrderStatus {
        COLLECTING,
        PAYMENT_IN_PROGRESS,
        PAID,
    }

    @PostMapping("/orders/{orderId}/payment")
    fun payOrder(@PathVariable orderId: UUID, @RequestParam deadline: Long): PaymentSubmissionDto {
        val paymentId = UUID.randomUUID()
        val order = orderRepository.findById(orderId)
            ?: throw IllegalArgumentException("No such order $orderId")


        val createdAt = orderPayer.processPayment(orderId, order.price, paymentId, deadline)
        orderRepository.markPaymentInProgress(orderId)
        return PaymentSubmissionDto(createdAt, paymentId)
    }

    @ExceptionHandler(PaymentBufferFull::class)
    fun bufferFull() = ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS).build<Void>()

    @ExceptionHandler(PaymentDeadlineExceeded::class)
    fun deadlineExceeded() = ResponseEntity.status(HttpStatus.REQUEST_TIMEOUT).build<Void>()

    @ExceptionHandler(PaymentBufferUnavailable::class)
    fun bufferUnavailable() = ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).build<Void>()

    class PaymentSubmissionDto(
        val timestamp: Long,
        val transactionId: UUID
    )
}
