package io.emeraldpay.dshackle.archive.runner

import io.emeraldpay.dshackle.archive.FileType
import io.emeraldpay.dshackle.archive.model.Chunk
import io.emeraldpay.dshackle.archive.model.Validator
import io.emeraldpay.dshackle.archive.storage.AvroReader
import io.emeraldpay.dshackle.archive.storage.SourceStorage
import io.emeraldpay.dshackle.archive.storage.StorageAccess
import org.apache.avro.file.SeekableInput
import org.jetbrains.annotations.NotNull
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import spock.lang.Specification

import java.time.Duration

class RunVerifySpec extends Specification {

    Validator<String> validator = new Validator<String>() {
        @Override
        String validate(String value) {
            if (value == "ok") return null
            return value
        }
    }

    def "validation returns on the first invalid item"() {
        setup:
        def runner = new RunVerifyImpl(Stub(StorageAccess))
        def counter = 0
        def inputs = Flux.fromIterable(["ok", "ok", "fail", "ok"])
            .doOnNext {
               counter++
            }

        when:
        def act = runner.validateWith(validator)
                .apply(inputs)
                .block(Duration.ofSeconds(1))
        then:
        act.failed
        counter == 3
    }

    def "validation process all items if no errors"() {
        setup:
        def runner = new RunVerifyImpl(Stub(StorageAccess))

        def counter = 0
        def inputs = Flux.fromIterable(["ok", "ok", "ok", "ok"])
                .doOnNext {
                    counter++
                }

        when:
        def act = runner.validateWith(validator)
                .apply(inputs)
                .block(Duration.ofSeconds(1))
        then:
        act.ok
        counter == 4
    }

    def "validate uses the file from storage"() {
        setup:
        def input = Stub(SeekableInput)
        def storage = Mock(StorageAccess)
        def runner = new RunVerifyImpl(storage)

        def counter = 0
        def inputs = Flux.fromIterable(["ok", "ok", "ok", "ok"])
                .doOnNext {
                    counter++
                }

        def file = new ScanningTools.FileChunk(FileType.BLOCKS, new Chunk(0, 10), "test.txt")
        def reader = Mock(AvroReader<String>)
        when:
        def act = runner.processWith(file, reader, validator)
                .block(Duration.ofSeconds(1))
        then:
        1 * storage.createReader("test.txt") >> input
        1 * reader.open(input) >> inputs

        act.ok
        counter == 4
    }

    def "keeps file if ok"() {
        setup:
        def storage = Mock(StorageAccess)
        def runner = new RunVerifyImpl(storage)
        def file = new ScanningTools.FileChunk(FileType.BLOCKS, new Chunk(0, 10), "test.txt")
        when:
        def act = runner.applyStatus(file)
                .apply(Mono.just(Status.ok()))
                .then(Mono.just(true))
                .block(Duration.ofSeconds(1))
        then:
        0 * storage.deleteArchives(_)
    }

    def "deletes file is error"() {
        setup:
        def storage = Mock(StorageAccess)
        def runner = new RunVerifyImpl(storage)
        def file = new ScanningTools.FileChunk(FileType.BLOCKS, new Chunk(0, 10), "test.txt")
        when:
        def act = runner.applyStatus(file)
            .apply(Mono.just(Status.err("test")))
            .then(Mono.just(true))
            .block(Duration.ofSeconds(1))
        then:
        1 * storage.deleteArchives(["test.txt"]) >> Mono.just(0).then()
    }

    class RunVerifyImpl extends RunVerifyBase {

        RunVerifyImpl(@NotNull StorageAccess storage) {
            super(storage)
        }
    }
}
