package ru.quipy.payments.logic

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.Collections
import java.util.concurrent.Callable
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

class PaymentAccountCallLimiterTest {
    @Test
    fun `does not exceed the account rate in any rolling second`() {
        val limiter = PaymentAccountCallLimiter(ratePerSecond = 5, parallelRequests = 10)
        val starts = Collections.synchronizedList(mutableListOf<Long>())
        val pool = Executors.newFixedThreadPool(10)
        try {
            val futures = (1..10).map {
                pool.submit(Callable {
                    limiter.executeBefore(System.currentTimeMillis() + 5_000) {
                        starts.add(System.nanoTime())
                        Unit
                    }
                })
            }
            futures.forEach { assertEquals(Unit, it.get(5, TimeUnit.SECONDS)) }

            val sorted = starts.sorted()
            assertEquals(10, sorted.size)
            for (i in 5 until sorted.size) {
                assertTrue(sorted[i] - sorted[i - 5] >= TimeUnit.SECONDS.toNanos(1))
            }
        } finally {
            pool.shutdownNow()
        }
    }

    @Test
    fun `limits concurrent calls and stops waiting at the deadline`() {
        val limiter = PaymentAccountCallLimiter(ratePerSecond = 100, parallelRequests = 2)
        val bothStarted = CountDownLatch(2)
        val release = CountDownLatch(1)
        val active = AtomicInteger()
        val maximum = AtomicInteger()
        val pool = Executors.newFixedThreadPool(3)
        try {
            val holders = (1..2).map {
                pool.submit(Callable {
                    limiter.executeBefore(System.currentTimeMillis() + 5_000) {
                        maximum.accumulateAndGet(active.incrementAndGet(), ::maxOf)
                        bothStarted.countDown()
                        release.await(5, TimeUnit.SECONDS)
                        active.decrementAndGet()
                        Unit
                    }
                })
            }
            assertTrue(bothStarted.await(2, TimeUnit.SECONDS))

            val expired = pool.submit(Callable {
                limiter.executeBefore(System.currentTimeMillis() + 50) {
                    throw AssertionError("A timed-out call reached the provider")
                }
            })
            assertNull(expired.get(2, TimeUnit.SECONDS))
            assertEquals(2, maximum.get())

            release.countDown()
            holders.forEach { assertEquals(Unit, it.get(2, TimeUnit.SECONDS)) }
            assertEquals(Unit, limiter.executeBefore(System.currentTimeMillis() + 1_000) {})
        } finally {
            release.countDown()
            pool.shutdownNow()
        }
    }

    @Test
    fun `releases the concurrent slot when a provider call fails`() {
        val limiter = PaymentAccountCallLimiter(ratePerSecond = 100, parallelRequests = 1)
        assertThrows(IllegalStateException::class.java) {
            limiter.executeBefore(System.currentTimeMillis() + 1_000) {
                throw IllegalStateException("provider failure")
            }
        }
        assertEquals(Unit, limiter.executeBefore(System.currentTimeMillis() + 1_000) {})
    }
}
