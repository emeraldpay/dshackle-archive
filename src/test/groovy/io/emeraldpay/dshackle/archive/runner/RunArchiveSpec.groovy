package io.emeraldpay.dshackle.archive.runner

import io.emeraldpay.dshackle.archive.BlocksRange
import io.emeraldpay.dshackle.archive.config.RunConfig
import io.emeraldpay.dshackle.archive.storage.CompleteWriter
import io.emeraldpay.dshackle.archive.storage.FilenameGenerator
import io.emeraldpay.dshackle.archive.storage.StorageAccess
import io.emeraldpay.dshackle.archive.storage.TargetStorage
import reactor.core.publisher.Flux
import spock.lang.Specification

import java.time.Duration

class RunArchiveSpec extends Specification {

    def "Doesn't list archive if it should not continue"() {
        setup:
        def config = RunConfig.default()
        def targetStorage = Mock(TargetStorage)
        def runArchive = new RunArchive(
                Stub(BlockSource), Stub(CompleteWriter), Stub(BlocksRange),
                config, targetStorage,
                Stub(FilenameGenerator)
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
        def runArchive = new RunArchive(
                Stub(BlockSource), Stub(CompleteWriter),
                new BlocksRange(config.range), config, targetStorage,
                filenameGenerator
        )

        when:
        runArchive.checkStartBlock().block(Duration.ofSeconds(1))
        then:
        1 * targetStorage.current >> storageAccess
        3 * storageAccess.listArchive(_) >>> [
                Flux.fromIterable(["range"]), Flux.empty(), Flux.empty()
        ]
    }
}
