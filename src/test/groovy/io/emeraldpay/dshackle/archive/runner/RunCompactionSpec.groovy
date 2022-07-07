package io.emeraldpay.dshackle.archive.runner

import io.emeraldpay.dshackle.archive.BlocksRange
import io.emeraldpay.dshackle.archive.config.RunConfig
import io.emeraldpay.dshackle.archive.model.Chunk
import io.emeraldpay.dshackle.archive.storage.CompleteWriter
import io.emeraldpay.dshackle.archive.storage.FilenameGenerator
import io.emeraldpay.dshackle.archive.storage.SourceStorage
import io.emeraldpay.dshackle.archive.storage.BlocksReader
import io.emeraldpay.dshackle.archive.storage.TransactionsReader
import reactor.core.publisher.Flux
import spock.lang.Specification

import java.nio.file.Path

class RunCompactionSpec extends Specification {

    def "Groups by a chunk"() {
        setup:
        def range = new BlocksRange(
                RunConfig.Range.forRange(1, 10, 2)
        )
        def filenameGenerator = Mock(FilenameGenerator) {
            _ * isSingle(_ as String) >> true
            _ * parseRange("file_1") >> new Chunk(1, 1)
            _ * parseRange("file_2") >> new Chunk(2, 1)
            _ * parseRange("file_3") >> new Chunk(3, 1)
            _ * parseRange("file_4") >> new Chunk(4, 1)
            _ * parseRange("file_5") >> new Chunk(5, 1)
        }
        def runCompaction = new RunCompaction(
                Stub(CompleteWriter), RunConfig.default(),
                range, filenameGenerator,
                Stub(SourceStorage), Stub(TransactionsReader), Stub(BlocksReader)
        )

        def files = [
                "file_1", "file_2", "file_3", "file_4", "file_5"
        ].collect {
            Path.of(it)
        }

        when:
        def act = runCompaction.groupByChunk(Flux.fromIterable(files))
            .flatMap { it.collectList() }
            .collectList()
            .block()

        then:
        act.size() == 3
        act[0].collect {it.toFile().path } == ["file_1"]
        act[1].collect {it.toFile().path } == ["file_2", "file_3"]
        act[2].collect {it.toFile().path } == ["file_4", "file_5"]
    }
}
