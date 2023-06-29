package io.emeraldpay.dshackle.archive.runner

import io.emeraldpay.dshackle.archive.BlocksRange
import io.emeraldpay.dshackle.archive.FileType
import io.emeraldpay.dshackle.archive.config.RunConfig
import io.emeraldpay.dshackle.archive.model.Chunk
import reactor.core.publisher.Flux
import spock.lang.Specification

class RunFixLogicSpec extends Specification {

    def "Process ranges of full archives"() {
        setup:
        def range = [
                new ScanningTools.FileChunk(FileType.TRANSACTIONS, new Chunk(0, 10), "0_9.txes.avro"),
                new ScanningTools.FileChunk(FileType.BLOCKS, new Chunk(0, 10), "0_9.blocks.avro"),

                new ScanningTools.FileChunk(FileType.TRANSACTIONS, new Chunk(10, 10), "10_19.txes.avro"),
                new ScanningTools.FileChunk(FileType.BLOCKS, new Chunk(10, 10), "0_19.blocks.avro"),

                new ScanningTools.FileChunk(FileType.TRANSACTIONS, new Chunk(20, 10), "20_29.txes.avro"),
                new ScanningTools.FileChunk(FileType.BLOCKS, new Chunk(20, 10), "20_29.blocks.avro"),
        ]

        def runFix = new RunFixLogic()

        when:

        def archived = Flux.fromIterable(range)
            .transform(runFix.fullyArchived())
            .collectList().block()

        then:
        archived.size() == 3
        archived[0] == new Chunk(0, 10)
        archived[1] == new Chunk(10, 10)
        archived[2] == new Chunk(20, 10)
    }

    def "Consider missing if no txes for a chunk"() {
        setup:
        def range = [
                new ScanningTools.FileChunk(FileType.TRANSACTIONS, new Chunk(0, 10), "0_9.txes.avro"),
                new ScanningTools.FileChunk(FileType.BLOCKS, new Chunk(0, 10), "0_9.blocks.avro"),

                new ScanningTools.FileChunk(FileType.BLOCKS, new Chunk(10, 10), "0_19.blocks.avro"),

                new ScanningTools.FileChunk(FileType.TRANSACTIONS, new Chunk(20, 10), "20_29.txes.avro"),
                new ScanningTools.FileChunk(FileType.BLOCKS, new Chunk(20, 10), "20_29.blocks.avro"),
        ]

        def runFix = new RunFixLogic()

        when:

        def archived = Flux.fromIterable(range)
                .transform(runFix.fullyArchived())
                .collectList().block()

        then:
        archived.size() == 2
        archived[0] == new Chunk(0, 10)
        archived[1] == new Chunk(20, 10)
    }

    def "Consider missing if no blocks for a chunk"() {
        setup:
        def range = [
                new ScanningTools.FileChunk(FileType.TRANSACTIONS, new Chunk(0, 10), "0_9.txes.avro"),
                new ScanningTools.FileChunk(FileType.BLOCKS, new Chunk(0, 10), "0_9.blocks.avro"),

                new ScanningTools.FileChunk(FileType.TRANSACTIONS, new Chunk(10, 10), "10_19.txes.avro"),
                new ScanningTools.FileChunk(FileType.BLOCKS, new Chunk(10, 10), "0_19.blocks.avro"),

                new ScanningTools.FileChunk(FileType.TRANSACTIONS, new Chunk(20, 10), "20_29.txes.avro"),
        ]

        def runFix = new RunFixLogic()

        when:

        def archived = Flux.fromIterable(range)
                .transform(runFix.fullyArchived())
                .collectList().block()

        then:
        archived.size() == 2
        archived[0] == new Chunk(0, 10)
        archived[1] == new Chunk(10, 10)
    }

    def "No broken for a full archive"() {
        setup:
        def range = [
                new Chunk(0, 10),
                new Chunk(10, 10),
                new Chunk(20, 10),
        ]

        def runFix = new RunFixLogic()

        when:

        def target = new Chunk(0, 30)
        def archived = Flux.fromIterable(range)
                .transform(runFix.findBroken(target))
                .collectList().block()

        then:
        archived.size() == 0
    }

    def "Broken when no blocks in between"() {
        setup:
        def range = [
                new Chunk(0, 10),
                new Chunk(20, 10),
        ]

        def runFix = new RunFixLogic()

        when:

        def target = new Chunk(0, 30)
        def archived = Flux.fromIterable(range)
                .transform(runFix.findBroken(target))
                .collectList().block()

        then:
        archived.size() == 1
        archived[0] == new Chunk(10, 10)
    }

    def "Broken when no blocks at the beginning"() {
        setup:
        def range = [
                new Chunk(10, 10),
                new Chunk(20, 10),
        ]

        def runFix = new RunFixLogic()

        when:

        def target = new Chunk(0, 30)
        def archived = Flux.fromIterable(range)
                .transform(runFix.findBroken(target))
                .collectList().block()

        then:
        archived.size() == 1
        archived[0] == new Chunk(0, 10)
    }

    def "Broken when no blocks at the end"() {
        setup:
        def range = [
                new Chunk(0, 10),
                new Chunk(10, 10),
        ]

        def runFix = new RunFixLogic()

        when:

        def target = new Chunk(0, 30)
        def archived = Flux.fromIterable(range)
                .transform(runFix.findBroken(target))
                .collectList().block()

        then:
        archived.size() == 1
        archived[0] == new Chunk(20, 10)
    }


    def "Broken when multiple missing"() {
        setup:
        def range = [
                new Chunk(0, 10),
                new Chunk(10, 10),
                new Chunk(30, 5),
                new Chunk(40, 10),
                new Chunk(60, 10),
        ]

        def runFix = new RunFixLogic()

        when:

        def target = new Chunk(0, 100)
        def archived = Flux.fromIterable(range)
                .transform(runFix.findBroken(target))
                .collectList().block()

        then:
        archived.size() == 4
        archived[0] == new Chunk(20, 10)
        archived[1] == new Chunk(35, 5)
        archived[2] == new Chunk(50, 10)
        archived[3] == new Chunk(70, 30)
    }

    def "Align correctly"() {
        setup:
        def range = [
                new Chunk(0, 10),
                new Chunk(10, 10),
                new Chunk(30, 5),
                new Chunk(40, 10),
                new Chunk(60, 10),
        ]

        def runFix = new RunFixLogic()

        when:

        def target = new Chunk(0, 100)
        def blocksRange = new BlocksRange(RunConfig.Range.forRange(0, 100, 100))
        def aligned = Flux.fromIterable(range)
                .transform(runFix.findBroken(target))
                .transform(runFix.align(blocksRange))
                .collectList().block()

        then:
        aligned.size() == 4
        aligned[0] == new Chunk(20, 10)
        aligned[1] == new Chunk(35, 5)
        aligned[2] == new Chunk(50, 10)
        aligned[3] == new Chunk(70, 30)
    }
}
