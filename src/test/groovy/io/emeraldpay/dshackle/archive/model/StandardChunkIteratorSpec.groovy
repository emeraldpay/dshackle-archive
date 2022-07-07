package io.emeraldpay.dshackle.archive.model

import spock.lang.Specification

class StandardChunkIteratorSpec extends Specification {

    def "All chunks from start"() {
        when:
        def iter = new StandardChunkIterator(0, 1000, 250)
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

    def "Continues if starts from middle"() {
        when:
        def iter = new StandardChunkIterator(75, 925, 250)
        def act = iter.chunks
        then:
        act.size() == 4
        with(act[0]) {
            it.startBlock == 75
            it.endBlock == 324
        }
        with(act[1]) {
            it.startBlock == 325
            it.endBlock == 574
        }
        with(act[2]) {
            it.startBlock == 575
            it.endBlock == 824
        }
        with(act[3]) {
            it.startBlock == 825
            it.endBlock == 999
        }
    }

    def "No chunks if zero length"() {
        when:
        def iter = new StandardChunkIterator(2199999, 0, 5000)
        def act = iter.chunks
        then:
        act.size() == 0
    }

    def "Exact chunk at the end"() {
        when:
        def iter = new StandardChunkIterator(2199999, 1, 5000)
        def act = iter.chunks
        then:
        act.size() == 1
        with(act[0]) {
            it.startBlock == 2_199_999
            it.endBlock == 2_199_999
            it.length == 1
        }
    }
}
