package io.emeraldpay.dshackle.archive.notify

import io.emeraldpay.dshackle.archive.FileType
import reactor.core.publisher.Mono
import spock.lang.Specification

import java.time.Instant

class MultiNotifierSpec extends Specification {

    def "Uses all delegates"() {
        setup:
        def send1 = Mock(Notifier)
        def send2 = Mock(Notifier)
        def multi = new MultiNotifier([send1, send2])
        def event = new Notifier.ArchiveCreated(
                "", Instant.now(), "test", FileType.TRANSACTIONS, 100, 100, "test"
        )
        when:
        multi.onCreated(event).block()
        then:
        1 * send1.onCreated(event) >> Mono.just(true).then()
        1 * send2.onCreated(event) >> Mono.just(true).then()
    }

    def "Continue if one delegates failed"() {
        setup:
        def send1 = Mock(Notifier)
        def send2 = Mock(Notifier)
        def multi = new MultiNotifier([send1, send2])
        def event = new Notifier.ArchiveCreated(
                "", Instant.now(), "test", FileType.TRANSACTIONS, 100, 100, "test"
        )
        when:
        multi.onCreated(event).block()
        then:
        1 * send1.onCreated(event) >> Mono.error(new RuntimeException())
        1 * send2.onCreated(event) >> Mono.just(true).then()
    }
}
