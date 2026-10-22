package ru.quipy.payments.logic

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

class PaymentBufferTest {
    @Test
    fun `full buffer rejects immediately and exposes queue gauges`() {
        val registry = SimpleMeterRegistry()
        val buffer = PaymentBuffer(1, 1, registry)
        val started = CountDownLatch(1)
        val release = CountDownLatch(1)
        try {
            buffer.submit(Long.MAX_VALUE) {
                started.countDown()
                release.await(5, TimeUnit.SECONDS)
            }
            assertTrue(started.await(2, TimeUnit.SECONDS))
            buffer.submit(Long.MAX_VALUE) {}
            assertEquals(1.0, registry.get("payment.buffer.queued").gauge().value())
            assertEquals(1.0, registry.get("payment.buffer.active").gauge().value())
            assertTimeoutPreemptively(java.time.Duration.ofSeconds(1)) {
                assertThrows(PaymentBufferFull::class.java) { buffer.submit(Long.MAX_VALUE) {} }
            }
            assertEquals(1.0, registry.get("payment.buffer.rejected").counter().count())
        } finally {
            release.countDown()
            buffer.close()
        }
        assertEquals(2.0, registry.get("payment.buffer.tasks").tag("outcome", "completed").counter().count())
        assertThrows(PaymentBufferUnavailable::class.java) { buffer.submit(Long.MAX_VALUE) {} }
    }

    @Test
    fun `expired queued work is skipped and task failure does not stop the worker`() {
        val registry = SimpleMeterRegistry()
        val buffer = PaymentBuffer(1, 2, registry)
        val started = CountDownLatch(1)
        val release = CountDownLatch(1)
        val ran = AtomicBoolean()
        try {
            assertThrows(PaymentDeadlineExceeded::class.java) { buffer.submit(0) {} }
            buffer.submit(Long.MAX_VALUE) {
                started.countDown()
                release.await(5, TimeUnit.SECONDS)
                error("test failure")
            }
            assertTrue(started.await(2, TimeUnit.SECONDS))
            buffer.submit(System.currentTimeMillis() + 100) { ran.set(true) }
            buffer.submit(Long.MAX_VALUE) { ran.set(true) }
            Thread.sleep(150)
        } finally {
            release.countDown()
            buffer.close()
        }
        assertTrue(ran.get())
        assertEquals(1.0, registry.get("payment.buffer.tasks").tag("outcome", "expired").counter().count())
        assertEquals(1.0, registry.get("payment.buffer.tasks").tag("outcome", "failed").counter().count())
        assertEquals(1.0, registry.get("payment.buffer.tasks").tag("outcome", "completed").counter().count())
    }
}
