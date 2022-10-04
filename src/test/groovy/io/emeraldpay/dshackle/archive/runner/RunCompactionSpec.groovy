package io.emeraldpay.dshackle.archive.runner

import io.emeraldpay.dshackle.archive.BlocksRange
import io.emeraldpay.dshackle.archive.avro.Block
import io.emeraldpay.dshackle.archive.avro.Transaction
import io.emeraldpay.dshackle.archive.config.RunConfig
import io.emeraldpay.dshackle.archive.model.Chunk
import io.emeraldpay.dshackle.archive.storage.CompleteWriter
import io.emeraldpay.dshackle.archive.storage.FilenameGenerator
import io.emeraldpay.dshackle.archive.storage.SourceStorage
import io.emeraldpay.dshackle.archive.storage.BlocksReader
import io.emeraldpay.dshackle.archive.storage.TransactionsReader
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import spock.lang.Specification

import java.nio.file.Path
import java.time.Duration

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
                Stub(SourceStorage), Stub(TransactionsReader), Stub(BlocksReader),
                null
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

    def "Filter blocks only not forked"() {
        setup:
        def inputs = [
                [100, "0xf0b553f4cf09da478cb3ff0b078adeb99d4a5ae9eb0fb1c5c93b944f5adb10ed"],
                [101, "0xa591080c5cb529427592eb2bd38db6bc451cc4915130da16fa50d51926dde8f0"],
                [102, "0xe2bcd5f4c3787499f648ac3904bbe0c0a8b51cca948b3443361a7e9a580c12f4"],
                [102, "0x56bc0637f146e560119a5ee5d3beade96ae60e231aa6a6b252ae2802270ee01d"]
        ]

        def source = Mock(BlockSource) {
            1 * it.getBlockIdAtHeight(100L) >> Mono.just(inputs[0][1] as String)
            1 * it.getBlockIdAtHeight(101L) >> Mono.just(inputs[1][1] as String)
            (1..2) * it.getBlockIdAtHeight(102L) >> Mono.just(inputs[3][1] as String)
        }
        def filter = new RunCompaction.ForkFilter(source)

        when:
        def act = Flux.fromIterable(
                inputs.collect { ref -> new Block().tap {
                    it.height = ref[0] as Long
                    it.blockId = ref[1]
                }}
        ).transform(filter.filterBlocks())
            .collectList().block(Duration.ofSeconds(1))

        then:
        act.collect { it.blockId } == [
                "0xf0b553f4cf09da478cb3ff0b078adeb99d4a5ae9eb0fb1c5c93b944f5adb10ed",
                "0xa591080c5cb529427592eb2bd38db6bc451cc4915130da16fa50d51926dde8f0",
                "0x56bc0637f146e560119a5ee5d3beade96ae60e231aa6a6b252ae2802270ee01d"
        ]
    }

    def "Filter transactions only not forked"() {
        setup:
        def inputs = [
                [101, "0xa591080c5cb529427592eb2bd38db6bc451cc4915130da16fa50d51926dde8f0", "0x71a324e6e4ddfe1aa635302dd688afb8a970abb4ef827b6fcd38fbd329d8e1ec"],

                [102, "0xe2bcd5f4c3787499f648ac3904bbe0c0a8b51cca948b3443361a7e9a580c12f4", "0xa34027ec1f2b8b83b4294842048fadea0c228ccde8b695e72bcd9c9b57dfe01e"],
                [102, "0xe2bcd5f4c3787499f648ac3904bbe0c0a8b51cca948b3443361a7e9a580c12f4", "0xcc56c1458b44436df3d6acc0d391b2ea873c5cfbfebe7ee00830f9ee26e1e51e"],

                [102, "0x56bc0637f146e560119a5ee5d3beade96ae60e231aa6a6b252ae2802270ee01d", "0xcc56c1458b44436df3d6acc0d391b2ea873c5cfbfebe7ee00830f9ee26e1e51e"],
                [102, "0x56bc0637f146e560119a5ee5d3beade96ae60e231aa6a6b252ae2802270ee01d", "0xf031a9c50185616006a7e5840388e0bdc170d86e9081c17e43c70a4956d912f1"],
                [102, "0x56bc0637f146e560119a5ee5d3beade96ae60e231aa6a6b252ae2802270ee01d", "0xa3f3a0d38c232f93cb6d2fa6dabab5bf0dab1471bdd28933cd14132a2e0ae7f3"],
        ]

        def source = Mock(BlockSource) {
            1 * it.getBlockIdAtHeight(101L) >> Mono.just("0xa591080c5cb529427592eb2bd38db6bc451cc4915130da16fa50d51926dde8f0")
            (1..2) * it.getBlockIdAtHeight(102L) >> Mono.just("0x56bc0637f146e560119a5ee5d3beade96ae60e231aa6a6b252ae2802270ee01d")
        }
        def filter = new RunCompaction.ForkFilter(source)

        when:
        def act = Flux.fromIterable(
                inputs.collect { ref -> new Transaction().tap {
                    it.height = ref[0] as Long
                    it.blockId = ref[1]
                    it.txid = ref[2]
                }}
        ).transform(filter.filterTxes())
                .collectList().block(Duration.ofSeconds(1))

        then:
        act.collect { it.txid } == [
                "0x71a324e6e4ddfe1aa635302dd688afb8a970abb4ef827b6fcd38fbd329d8e1ec",
                "0xcc56c1458b44436df3d6acc0d391b2ea873c5cfbfebe7ee00830f9ee26e1e51e",
                "0xf031a9c50185616006a7e5840388e0bdc170d86e9081c17e43c70a4956d912f1",
                "0xa3f3a0d38c232f93cb6d2fa6dabab5bf0dab1471bdd28933cd14132a2e0ae7f3"
        ]
    }

}
