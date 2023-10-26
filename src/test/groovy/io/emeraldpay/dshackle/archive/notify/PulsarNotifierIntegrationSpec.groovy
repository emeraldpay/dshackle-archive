package io.emeraldpay.dshackle.archive.notify

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.pulsar.client.api.PulsarClient
import org.apache.pulsar.client.api.Schema
import org.testcontainers.containers.PulsarContainer
import org.testcontainers.utility.DockerImageName
import spock.lang.Specification

import java.time.Duration

class PulsarNotifierIntegrationSpec extends Specification {

    final String topic = "test_topic"

    PulsarContainer pulsar
    PulsarNotifier notifier

    def setup() {
        pulsar = new PulsarContainer(DockerImageName.parse("apachepulsar/pulsar:3.0.0"));
        pulsar.start()

        notifier = new PulsarNotifier(
                pulsar.getPulsarBrokerUrl(),
                topic,
                new ObjectMapper()
        )
        notifier.start()
    }

    def cleanup() {
        notifier.stop()
        pulsar.stop()
    }

    def "Can send a message"() {
        setup:
        PulsarClient client = PulsarClient.builder().serviceUrl(pulsar.getPulsarBrokerUrl()).build()
        def consumer = client.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("test_send")
                .subscribe()

        when:
        notifier.onCreated('{"foo":"bar"}').block(Duration.ofSeconds(1))
        def act = consumer.receive()

        then:
        act != null
        act.getValue() == '{"foo":"bar"}'
    }

}
