package io.emeraldpay.dshackle.archive.storage

import io.emeraldpay.dshackle.archive.storage.gcp.GSStorageAccess
import io.emeraldpay.dshackle.archive.storage.gcp.GoogleStorage
import reactor.core.publisher.Flux
import spock.lang.Specification

class GSStorageAccessSpec extends Specification {

    def "Scan all Level 0 dirs until none exist"() {
        setup:
        def filenameGenerator = new FilenameGenerator("v0", "dir", 1_000_000, 1000)
        def storage = Spy(new GSStorageAccess(filenameGenerator, Stub(GoogleStorage)))

        when:
        def results = storage.nextLevel0(3)
                .collectList().block()

        then:
        1 * storage.listArchiveLevel0(3_000_000) >> Flux.fromIterable(["3M/0.txt", "3M/1.txt"])
        1 * storage.listArchiveLevel0(4_000_000) >> Flux.fromIterable(["4M/0.txt", "4M/1.txt"])
        1 * storage.listArchiveLevel0(5_000_000) >> Flux.empty()
        results == ["3M/0.txt", "3M/1.txt", "4M/0.txt", "4M/1.txt"]
    }

}
