package io.emeraldpay.dshackle.archive.storage

import org.apache.avro.file.SeekableByteArrayInput
import reactor.core.publisher.Flux
import spock.lang.Specification

import java.time.Duration

class AvroPublisherSpec extends Specification {

    def dir = "fullAvroFiles"

    def "Reads whole file with txes"() {
        setup:
        def name = "015437941.txes.avro"
        def file = this.class.getResourceAsStream("/$dir/ethereum/$name").readAllBytes()

        when:
        def reader = new TransactionsReader().open(new SeekableByteArrayInput(file))
        def act = Flux.from(reader).collectList().block(Duration.ofSeconds(1))

        then:
        act.size() == 23
        reader instanceof AvroPublisher
        for (i in 0..22) {
            act[i].index == i
        }
    }

}
