package io.emeraldpay.dshackle.archive.storage

import spock.lang.Specification

class FilenameGeneratorSpec extends Specification {

    FilenameGenerator generator = new FilenameGenerator("", "test/", 1_000_000, 1_000)

    def "Parse ranges"() {
        expect:
        def range = generator.parseRange(filename)
        range != null
        range.startBlock == start
        range.length == length
        where:
        filename                                          | start    | length
        "range-012546000_012547000.blocks.avro"           | 12546000 | 1001
        "range-012546000_012546999.blocks.avro"           | 12546000 | 1000
        "range-012546000_012546999.blocks.v20220102.avro" | 12546000 | 1000
    }

    def "Parse ranges withing a dir"() {
        expect:
        def range = generator.parseRange(filename)
        range != null
        range.startBlock == start
        range.length == length
        where:
        filename                                                    | start    | length
        "012000000/range-012546000_012546999.blocks.avro"           | 12546000 | 1000
        "012000000/range-012546000_012546999.blocks.v20220102.avro" | 12546000 | 1000
    }

    def "Parse individual files"() {
        expect:
        def range = generator.parseRange(filename)
        range != null
        range.startBlock == start
        range.length == length
        where:
        filename                         | start    | length
        "012000000.block.avro"           | 12000000 | 1
        "012000000.block.v20220102.avro" | 12000000 | 1
    }

    def "Parse individual files withing a dir"() {
        expect:
        def range = generator.parseRange(filename)
        range != null
        range.startBlock == start
        range.length == length
        where:
        filename                                             | start    | length
        "012000000/012001000/012000005.block.avro"           | 12000005 | 1
        "012000000/012001000/012000012.block.v20220102.avro" | 12000012 | 1
    }
}
