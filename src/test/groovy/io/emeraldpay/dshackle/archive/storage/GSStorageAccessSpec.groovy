package io.emeraldpay.dshackle.archive.storage

import com.google.cloud.storage.BlobInfo
import io.emeraldpay.dshackle.archive.storage.gcp.GSStorageAccess
import io.emeraldpay.dshackle.archive.storage.gcp.GoogleStorage
import reactor.core.publisher.Flux
import spock.lang.Specification

class GSStorageAccessSpec extends Specification {

    def "Scan all Level 0 dirs until none exist"() {
        setup:
        def filenameGenerator = new FilenameGenerator("v0", "dir", 1_000_000, 1000)
        def storage = Spy(new GSStorageAccess(filenameGenerator, Stub(GoogleStorage)))

        when:
        def results = storage.nextLevel0(3)
                .collectList().block()

        then:
        1 * storage.listArchiveLevel0(3_000_000) >> Flux.fromIterable(["3M/0.txt", "3M/1.txt"])
        1 * storage.listArchiveLevel0(4_000_000) >> Flux.fromIterable(["4M/0.txt", "4M/1.txt"])
        1 * storage.listArchiveLevel0(5_000_000) >> Flux.empty()
        results == ["3M/0.txt", "3M/1.txt", "4M/0.txt", "4M/1.txt"]
    }

    def "Scan all Level 0 and level 1 within required range"() {
        setup:
        def filenameGenerator = new FilenameGenerator("v0", "dir/", 1_000_000, 1000)
        def storage = Spy(new GSStorageAccess(filenameGenerator, Stub(GoogleStorage)))

        when:
        def results = storage.listArchive(2_000_123)
                .collectList().block()

        then:
        1 * storage.query(
                new GSStorageAccess.ListQuery("/dir/002000000/", "/dir/002000000/002000000/002000123.block.v0.avro", "/dir/002000000/999999999")
        ) >> Flux.fromIterable([BlobInfo.newBuilder("bucket", "/dir/2M/0.txt").build(), BlobInfo.newBuilder("bucket", "/dir/2M/1.txt").build()])
        1 * storage.query(
                new GSStorageAccess.ListQuery("/dir/002000000/", "/dir/002000000/range-002000123_002000122.block.v0.avro", null)
        ) >> Flux.empty()
        1 * storage.query(
                new GSStorageAccess.ListQuery("/dir/003000000/", "/dir/003000000/003000000/003000000.block.v0.avro", "/dir/003000000/999999999")
        ) >> Flux.fromIterable([BlobInfo.newBuilder("bucket", "/dir/3M/0.txt").build(), BlobInfo.newBuilder("bucket", "/dir/3M/1.txt").build()])
        1 * storage.query(
                new GSStorageAccess.ListQuery("/dir/003000000/", "/dir/003000000/range-003000000_002999999.block.v0.avro", null)
        ) >> Flux.fromIterable([BlobInfo.newBuilder("bucket", "/dir/3M/range-1-2.txt").build()])
        results == ["dir/2M/0.txt", "dir/2M/1.txt", "dir/3M/0.txt", "dir/3M/1.txt", "dir/3M/range-1-2.txt"]
    }

}
