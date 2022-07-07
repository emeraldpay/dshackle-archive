package io.emeraldpay.dshackle.archive.model

import spock.lang.Specification

class ChunkSpec extends Specification {

    def "Cannot have negative length"() {
        when:
        new Chunk(100, -1)
        then:
        thrown(IllegalStateException)
    }

    def "Cannot have negative start block"() {
        when:
        new Chunk(-5, 100)
        then:
        thrown(IllegalStateException)
    }

    def "Intersection"() {
        setup:
        def chunk = new Chunk(20_000, 1_000)
        expect:
        def o = new Chunk(start, len)
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

    def "No intersection"() {
        setup:
        def chunk = new Chunk(20_000, 1_000)
        expect:
        def o = new Chunk(start, len)
        !o.intersects(chunk)
        !chunk.intersects(o)
        where:
        start  | len
        0      | 10_000
        0      | 20_000
        21_000 | 10
    }

    def "Continuity when intersects"() {
        setup:
        def other = [
                new Chunk(1_000, 500),
                new Chunk(2_000, 500),
                new Chunk(3_000, 500),
        ]
        def main = new Chunk(1_750, 500)
        when:
        def act = main.findContinuity(other)
        then:
        act != null
        act == other[1]
    }

    def "Continuity when follows on side"() {
        setup:
        def other = [
                new Chunk(1_000, 500),
                new Chunk(2_000, 500),
                new Chunk(3_000, 500),
        ]
        def main = new Chunk(1_500, 500)
        when:
        def act = main.findContinuity(other)
        then:
        act != null
        act == other[0]
    }

    def "Join two intersecting"() {
        setup:
        def a = new Chunk(1_000, 500)
        def b = new Chunk(1_250, 500)
        when:
        def act = a.join(b)
        then:
        act.startBlock == a.startBlock
        act.endBlock == b.endBlock
        act.startBlock == 1000
        act.endBlock == 1749
        act.length == 750
    }

    def "Join two following"() {
        setup:
        def a = new Chunk(1_000, 500)
        def b = new Chunk(1_500, 500)
        when:
        def act = a.join(b)
        then:
        act.startBlock == a.startBlock
        act.endBlock == b.endBlock
        act.startBlock == 1000
        act.endBlock == 1999
        act.length == 1000
    }

    def "Join same"() {
        setup:
        def a = new Chunk(1_000, 500)
        when:
        def act = a.join(a)
        then:
        act == a
    }

    def "No merge if no continuity"() {
        setup:
        def src = [
                new Chunk(1_000, 500),
                new Chunk(2_000, 500),
                new Chunk(3_000, 500),
        ]
        def base = new Chunk(1_750, 50)
        when:
        def act = base.mergeContinuing(src)
        then:
        act.toSorted {it.startBlock } == [src[0], base, src[1], src[2]]
    }

    def "Merge one intersected"() {
        setup:
        def src = [
                new Chunk(1_000, 500),
                new Chunk(2_000, 500),
                new Chunk(3_000, 500),
        ]
        def base = new Chunk(1_750, 300)
        def exp = base.join(src[1])
        when:
        def act = base.mergeContinuing(src)
        then:
        act.toSorted {it.startBlock } == [src[0], exp, src[2]]
    }

    def "Merge few intersected"() {
        setup:
        def src = [
                new Chunk(1_000, 500),
                new Chunk(2_000, 500),
                new Chunk(3_000, 500),
        ]
        def base = new Chunk(1_750, 1300)
        def exp = base.join(src[1]).join(src[2])
        when:
        def act = base.mergeContinuing(src)
        then:
        act.toSorted {it.startBlock } == [src[0], exp]
    }

}
