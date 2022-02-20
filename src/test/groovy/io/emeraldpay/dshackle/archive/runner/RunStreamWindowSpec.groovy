package io.emeraldpay.dshackle.archive.runner

import spock.lang.Specification

class RunStreamWindowSpec extends Specification {

    def "Single gap"() {
        setup:
        def window = new RunStream.ProcessingWindow(100, [802L, 803L, 810L])
        when:
        def gaps = window.getMissingGaps()
        println("Values: $gaps")
        then:
        gaps.size() > 0
        gaps == [804L, 805, 806, 807, 808, 809]
    }

    def "Large missing head"() {
        setup:
        def window = new RunStream.ProcessingWindow(100, [802L, 803L, 810L])
        when:
        def gaps = window.getMissingHead()
        println("Values: $gaps")
        then:
        gaps.size() > 0
        gaps.contains(711L)
        gaps.contains(801L)
        gaps.size() == 92
    }

}
