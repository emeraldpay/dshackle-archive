package io.emeraldpay.dshackle.archive.runner

import io.emeraldpay.dshackle.archive.model.Chunk
import io.emeraldpay.dshackle.archive.storage.FilenameGenerator
import io.emeraldpay.dshackle.archive.storage.SourceStorage
import io.emeraldpay.dshackle.archive.storage.StorageAccess
import reactor.core.publisher.Flux
import reactor.util.function.Tuples
import spock.lang.Specification

class ScanningToolsSpec extends Specification {

    def "Request until the range is processed"() {
        setup:
        def n = 0
        def files = Flux.generate { s ->
                if (n > 100) {
                    s.complete()
                } else {
                    s.next(n++)
                }
            }.map { Integer it ->
                Tuples.of(it * 1000, it * 1000 + 999)
            }.map {
                "range-${it.t1}_${it.t2}.txes.avro".toString()
            }

        def storage = Mock(SourceStorage) {
            1 * current >> Mock(StorageAccess) {
                1 * listArchive(_) >> files
            }
        }
        def tools = new ScanningTools(
                new FilenameGenerator("", "", 1000000, 1000),
                storage
        )

        when:
        def act = tools.scanArchives(
                new Chunk(10_000, 50_000)
        ).collectList().block()

        then:
        // each range is 1000 blocks, so it's 50 files
        act.size() == 50
        with(act.first()) {
            path == "range-10000_10999.txes.avro"
        }
        with(act.last()) {
            path == "range-59000_59999.txes.avro"
        }

        // make sure it doesn't request any other files besides the queried
        // 60xxx supposed to be the last range when it stops, but the N is already incremented, so for the test it's 61
        // and since it's not so strictly required let's allow to go one more file, so up it to 62
        n in 60..62
    }

}
