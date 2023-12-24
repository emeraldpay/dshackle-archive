package io.emeraldpay.dshackle.archive.storage

import spock.lang.Specification

class BucketPathSpec extends Specification {

    def "Removes double slash"() {
        setup:
        def path = new BucketPath("a/b//")
        when:
        def result = path.fullPathFor("/c.txt")
        then:
        result == "a/b/c.txt"
    }
}
