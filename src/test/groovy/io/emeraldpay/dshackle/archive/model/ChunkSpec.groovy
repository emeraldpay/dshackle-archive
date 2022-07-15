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

    def "Includes"() {
        setup:
        def chunk = new Chunk(20_000, 1_000)
        expect:
        def o = new Chunk(start, len)
        chunk.includes(o)
        where:
        start  | len
        20_000 | 1_000
        20_000 | 999
        20_000 | 100
        20_500 | 10
        20_999 | 1
    }

    def "Not includes"() {
        setup:
        def chunk = new Chunk(20_000, 1_000)
        expect:
        def o = new Chunk(start, len)
        !chunk.includes(o)
        where:
        start  | len
        20_000 | 1_001
        19_990 | 1_000
        10_000 | 1_000
        10_000 | 10_000
        10_000 | 15_000
    }

    def "Cut from the left"() {
        setup:
        def chunk = new Chunk(20_000, 1_000)
        expect:
        def o = new Chunk(start, len)
        def exp = new Chunk(expStart, expLen)
        chunk.cut(o) == exp
        where:
        start  | len        | expStart  | expLen
        19_000 | 1_000      | 20_000    | 1_000
        19_000 | 1_001      | 20_001    | 999
        19_000 | 1_100      | 20_100    | 900
        19_000 | 1_999      | 20_999    | 1
        19_000 | 900        | 20_000    | 1_000
    }

    def "Cut from the right"() {
        setup:
        def chunk = new Chunk(20_000, 1_000)
        expect:
        def o = new Chunk(start, len)
        def exp = new Chunk(expStart, expLen)
        chunk.cut(o) == exp
        where:
        start  | len        | expStart  | expLen
        21_000 | 1_000      | 20_000    | 1_000
        20_999 | 1          | 20_000    | 999
        20_900 | 100        | 20_000    | 900
        20_001 | 1_000      | 20_000    | 1
        21_001 | 1          | 20_000    | 1_000
    }

    def "Split in the beginning"() {
        setup:
        def chunk = new Chunk(20_000, 1_000)
        def middle = new Chunk(20_001, 99)
        when:
        def sides = chunk.splitBy(middle)
        then:
        with(sides.first) {
            it.startBlock == 20_000
            it.endBlock == 20_000
        }
        with(sides.second) {
            it.startBlock == 20_100
            it.endBlock == 20_999
        }
    }

    def "Split in the middle"() {
        setup:
        def chunk = new Chunk(20_000, 1_000)
        def middle = new Chunk(20_450, 100)
        when:
        def sides = chunk.splitBy(middle)
        then:
        with(sides.first) {
            it.startBlock == 20_000
            it.endBlock == 20_449
        }
        with(sides.second) {
            it.startBlock == 20_550
            it.endBlock == 20_999
        }
    }

    def "Split in the end"() {
        setup:
        def chunk = new Chunk(20_000, 1_000)
        def middle = new Chunk(20_900, 99)
        when:
        def sides = chunk.splitBy(middle)
        then:
        with(sides.first) {
            it.startBlock == 20_000
            it.endBlock == 20_899
        }
        with(sides.second) {
            it.startBlock == 20_999
            it.endBlock == 20_999
        }
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
