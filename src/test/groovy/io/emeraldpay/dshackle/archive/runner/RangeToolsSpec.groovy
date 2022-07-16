package io.emeraldpay.dshackle.archive.runner

import io.emeraldpay.dshackle.archive.BlocksRange
import io.emeraldpay.dshackle.archive.config.RunConfig
import io.emeraldpay.dshackle.archive.storage.FilenameGenerator
import io.emeraldpay.dshackle.archive.storage.StorageAccess
import io.emeraldpay.dshackle.archive.storage.TargetStorage
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import spock.lang.Specification

import java.time.Duration

class RangeToolsSpec extends Specification {

    def "Doesn't list archive if it should not continue"() {
        setup:
        def config = RunConfig.default()
        def targetStorage = Mock(TargetStorage)
        def runArchive = new RangeTools(
                Stub(BlocksRange),
                config, targetStorage,
                Stub(FilenameGenerator), Stub(BlockSource)
        )

        when:
        runArchive.checkStartBlock().block(Duration.ofSeconds(1))
        then:
        0 * targetStorage.current
    }

    def "List target storage for items if it should continue"() {
        setup:
        def config = RunConfig.default()
        config = config.withRange(
                RunConfig.Range.forRange(1000, 200, 100).withContinueFromLast(true)
        )
        def filenameGenerator = new FilenameGenerator("t", "test", 10000, 1000)

        def targetStorage = Mock(TargetStorage)
        def storageAccess = Mock(StorageAccess)
        def runArchive = new RangeTools(
                new BlocksRange(config.range), config, targetStorage,
                filenameGenerator, Stub(BlockSource)
        )

        when:
        runArchive.checkStartBlock().block(Duration.ofSeconds(1))
        then:
        1 * targetStorage.current >> storageAccess
        3 * storageAccess.listArchive(_) >>> [
                Flux.fromIterable(["range"]), Flux.empty(), Flux.empty()
        ]
    }

    def "For backwards use opposite direction from the current height"() {
        setup:
        def config = RunConfig.default()
        config = config.withRange(
                // which means 1000 to 1199 from current height
                RunConfig.Range.forRange(1000, 200, 100)
                        .withBackward(true)
        )

        def blockSource = Mock(BlockSource)
        def runArchive = new RangeTools(
                new BlocksRange(config.range), config,
                Stub(TargetStorage), Stub(FilenameGenerator),
                blockSource
        )

        when:
        def act = runArchive.checkStartBlock().block(Duration.ofSeconds(1))
        then:
        1 * blockSource.getCurrentHeight() >> Mono.just(2500L)
        act.startBlock == 1300
        act.length == 200
    }

    def "Don't got below zero with backwards"() {
        setup:
        def config = RunConfig.default()
        config = config.withRange(
                RunConfig.Range.forRange(1000, 200, 100)
                        .withBackward(true)
        )

        def blockSource = Mock(BlockSource)
        def runArchive = new RangeTools(
                new BlocksRange(config.range), config,
                Stub(TargetStorage), Stub(FilenameGenerator),
                blockSource
        )

        when:
        def act = runArchive.checkStartBlock().block(Duration.ofSeconds(1))
        then:
        1 * blockSource.getCurrentHeight() >> Mono.just(1100L)
        act.startBlock == 0
        act.length == 100
    }

    def "Backwards range is empty when it fully goes below a zero block"() {
        setup:
        def config = RunConfig.default()
        config = config.withRange(
                RunConfig.Range.forRange(1000, 200, 100)
                        .withBackward(true)
        )

        def blockSource = Mock(BlockSource)
        def runArchive = new RangeTools(
                new BlocksRange(config.range), config,
                Stub(TargetStorage), Stub(FilenameGenerator),
                blockSource
        )

        when:
        def act = runArchive.checkStartBlock().block(Duration.ofSeconds(1))
        then:
        1 * blockSource.getCurrentHeight() >> Mono.just(100L)
        act.startBlock == 0
        act.length == 0
    }
}
