package io.emeraldpay.dshackle.archive.config

import spock.lang.Specification

class RunConfigSpec extends Specification {

    def "Parse s3 url with port"() {
        when:
        def act = new RunConfig.ExportBucket("s3://test:1234/data/blockchain", null)

        then:
        act.bucket == "test"
        act.path == "/data/blockchain"
    }

    def "Parse long s3 url"() {
        when:
        def act = new RunConfig.ExportBucket("s3://test.s3.databases.host.local/data/blockchain", null)

        then:
        act.bucket == "test"
        act.path == "/data/blockchain"
    }

    def "Parse long GS url"() {
        when:
        def act = new RunConfig.ExportBucket("gs://BUCKET_NAME.storage.googleapis.com", null)

        then:
        act.bucket == "BUCKET_NAME"
        act.path == ""
    }

}
