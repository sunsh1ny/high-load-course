package ru.quipy.config

import jakarta.annotation.PostConstruct
import org.eclipse.jetty.http2.server.HTTP2CServerConnectionFactory
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.web.embedded.jetty.JettyServerCustomizer
import org.springframework.boot.web.embedded.jetty.JettyServletWebServerFactory
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import ru.quipy.core.EventSourcingServiceFactory
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.logic.PaymentAggregateState
import ru.quipy.streams.AggregateEventStreamManager
import java.util.*
@Configuration
class EventSourcingLibConfiguration {

    private val logger = LoggerFactory.getLogger(EventSourcingLibConfiguration::class.java)

    @Autowired
    private lateinit var eventSourcingServiceFactory: EventSourcingServiceFactory

    @Autowired
    private lateinit var eventStreamManager: AggregateEventStreamManager

    @Bean
    fun paymentsEsService() = eventSourcingServiceFactory.create<UUID, PaymentAggregate, PaymentAggregateState>()

    @PostConstruct
    fun init() {
        eventStreamManager.maintenance {
            onRecordHandledSuccessfully { streamName, eventName ->
                logger.debug("Stream $streamName successfully processed record of $eventName")
            }

            onBatchRead { streamName, batchSize ->
                logger.debug("Stream $streamName read batch size: $batchSize")
            }
        }
    }

    @Bean
    fun jettyServerCustomizer(): JettyServletWebServerFactory {
        val jettyServletWebServerFactory = JettyServletWebServerFactory()

        val c = JettyServerCustomizer {
            (it.connectors[0].getConnectionFactory("h2c") as HTTP2CServerConnectionFactory).maxConcurrentStreams = 10_000_000
        }

        jettyServletWebServerFactory.serverCustomizers.add(c)
        return jettyServletWebServerFactory
    }
}
