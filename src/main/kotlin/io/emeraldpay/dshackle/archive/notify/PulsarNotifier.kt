package io.emeraldpay.dshackle.archive.notify


import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.pulsar.client.api.PulsarClient
import org.apache.pulsar.client.api.Schema
import org.apache.pulsar.reactive.client.adapter.AdaptedReactivePulsarClientFactory
import org.apache.pulsar.reactive.client.api.MessageSpec
import org.apache.pulsar.reactive.client.api.ReactiveMessageSender
import org.slf4j.LoggerFactory
import org.springframework.context.Lifecycle
import reactor.core.publisher.Mono
import java.util.concurrent.atomic.AtomicReference


class PulsarNotifier(
    // ex: "pulsar://xxxx:6650"
    private val url: String,
    private val topic: String,
    objectMapper: ObjectMapper,
) : Notifier.JsonBased(objectMapper), Lifecycle {

    companion object {
        private val log = LoggerFactory.getLogger(PulsarNotifier::class.java)
    }

    private val connection = AtomicReference<Connection?>(null)

    override fun onCreated(archiveJson: String): Mono<Void> {
        val messageSender: ReactiveMessageSender<String> = connection.get()?.messageSender ?: return Mono.error(IllegalStateException("Pulsar Client is not started"))
        return messageSender
            .sendOne(MessageSpec.of(archiveJson))
            .then()
    }

    override fun start() {
        connection.updateAndGet { prev ->
            prev?.pulsarClient?.close()
            log.info("Connecting to Pulsar at $url to topic $topic")
            val pulsarClient = PulsarClient.builder()
                .serviceUrl(url)
                .build()
            val reactiveClient = AdaptedReactivePulsarClientFactory.create(pulsarClient)
            val messageSender = reactiveClient
                .messageSender(Schema.STRING)
                .cache(AdaptedReactivePulsarClientFactory.createCache())
                .topic(topic)
                .maxInflight(100)
                .build()
            Connection(pulsarClient, messageSender)
        }
    }

    override fun stop() {
        connection.updateAndGet { prev ->
            prev?.pulsarClient?.close()
            null
        }
    }

    override fun isRunning(): Boolean {
        return connection.get() != null
    }

    data class Connection(
        val pulsarClient: PulsarClient,
        val messageSender: ReactiveMessageSender<String>,
    )
}
