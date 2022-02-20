package io.emeraldpay.dshackle.archive


import spock.lang.Specification

class BlocksRangeSpec extends Specification {

    def "Chunk intersection"() {
        setup:
        def chunk = new BlocksRange.Chunk(20_000, 1_000)
        expect:
        def o = new BlocksRange.Chunk(start, len)
        o.intersects(chunk)
        chunk.intersects(o)
        where:
        start  | len
        0      | 100_000
        0      | 21_000
        0      | 20_500
        0      | 20_001
        10_000 | 11_000
        20_000 | 1_000
        20_000 | 5_000
        20_500 | 500
        20_500 | 1000
        20_999 | 10
        20_999 | 1
        20_500 | 10
    }

    def "Chunk not intersection"() {
        setup:
        def chunk = new BlocksRange.Chunk(20_000, 1_000)
        expect:
        def o = new BlocksRange.Chunk(start, len)
        !o.intersects(chunk)
        !chunk.intersects(o)
        where:
        start  | len
        0      | 10_000
        0      | 20_000
        21_000 | 10
    }
}
