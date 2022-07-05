package io.emeraldpay.dshackle.archive.storage

import io.emeraldpay.dshackle.archive.BlocksRange
import io.emeraldpay.dshackle.archive.config.RunConfig
import spock.lang.Specification

class RangeAccessSpec extends Specification {

    def "All potential heights in different chunks"() {
        def config = RunConfig.default()
        config = config.withRange(
                RunConfig.Range.forRange(1000, 300, 100).withContinueFromLast(true)
        )
        def rangeAccess = new RangeAccess(config)

        when:
        def heights = rangeAccess.findHeightsToCheck(new BlocksRange(config.range))

        then:
        // it's the current implementation, but it's not necessary to generate heights in the middle of chunks
        heights.toSorted() == [1000L, 1050, 1100, 1150, 1200, 1250, 1300]
    }
}
