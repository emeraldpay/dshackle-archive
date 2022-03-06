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

    def "AlignedChunkIterator from start"() {
        when:
        def iter = new BlocksRange.AlignedChunkIterator(0, 1000, 250)
        def act = iter.chunks
        then:
        act.size() == 4
        with(act[0]) {
            it.startBlock == 0
            it.endBlock == 249
        }
        with(act[1]) {
            it.startBlock == 250
            it.endBlock == 499
        }
        with(act[2]) {
            it.startBlock == 500
            it.endBlock == 749
        }
        with(act[3]) {
            it.startBlock == 750
            it.endBlock == 999
        }
    }

    def "AlignedChunkIterator not from start"() {
        when:
        def iter = new BlocksRange.AlignedChunkIterator(75, 925, 250)
        def act = iter.chunks
        then:
        act.size() == 4
        with(act[0]) {
            it.startBlock == 75
            it.endBlock == 249
        }
        with(act[1]) {
            it.startBlock == 250
            it.endBlock == 499
        }
        with(act[2]) {
            it.startBlock == 500
            it.endBlock == 749
        }
        with(act[3]) {
            it.startBlock == 750
            it.endBlock == 999
        }
    }

    def "AlignedChunkIterator crossing in the middle"() {
        when:
        def iter = new BlocksRange.AlignedChunkIterator(75, 900, 250)
        def act = iter.chunks
        then:
        act.size() == 4
        with(act[0]) {
            it.startBlock == 75
            it.endBlock == 249
        }
        with(act[1]) {
            it.startBlock == 250
            it.endBlock == 499
        }
        with(act[2]) {
            it.startBlock == 500
            it.endBlock == 749
        }
        with(act[3]) {
            it.startBlock == 750
            it.endBlock == 974
        }
    }
}
