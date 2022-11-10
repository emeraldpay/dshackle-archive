package io.emeraldpay.dshackle.archive.storage.fs

import reactor.core.publisher.Flux
import spock.lang.Specification

import java.nio.file.Files
import java.time.Duration

class FilesStorageAccessSpec extends Specification {

    def "Scanning a non-existing dir produces empty"() {
        setup:
        def dir = Files.createTempDirectory("dshackle-fs-test")
        def noneDir = dir.resolve("none")
        def publisher = new FilesStorageAccess.FilesPublisher(noneDir)
        when:
        def act = Flux.from(publisher)
            .collectList().block(Duration.ofSeconds(1))
        then:
        act == []
        !Files.exists(noneDir)
    }

    def "Scanning an empty dir produces empty"() {
        setup:
        def dir = Files.createTempDirectory("dshackle-fs-test")
        def publisher = new FilesStorageAccess.FilesPublisher(dir)
        when:
        def act = Flux.from(publisher)
                .collectList().block(Duration.ofSeconds(1))
        then:
        act == []
        Files.exists(dir)
        Files.isDirectory(dir)
    }

    def "Scan files in the top dir"() {
        setup:
        def dir = Files.createTempDirectory("dshackle-fs-test")
        Files.write(dir.resolve("test1.txt"), "1".getBytes())
        Files.write(dir.resolve("test2.txt"), "2".getBytes())

        def publisher = new FilesStorageAccess.FilesPublisher(dir)
        when:
        def act = Flux.from(publisher)
                .collectList().block(Duration.ofSeconds(1))
        then:
        act.collect {dir.relativize(it).toString() } == [
                "test1.txt", "test2.txt"
        ]
    }

    def "Scan files in the subdir"() {
        setup:
        def dir = Files.createTempDirectory("dshackle-fs-test")
        def subdir = dir.resolve("subdir")
        Files.createDirectory(subdir)
        Files.write(subdir.resolve("test1.txt"), "1".getBytes())
        Files.write(subdir.resolve("test2.txt"), "2".getBytes())

        def publisher = new FilesStorageAccess.FilesPublisher(dir)
        when:
        def act = Flux.from(publisher)
                .collectList().block(Duration.ofSeconds(1))
        then:
        act.collect {dir.relativize(it).toString() } == [
                "subdir/test1.txt", "subdir/test2.txt"
        ]
    }


    def "Scan files in different places"() {
        setup:
        def dir = Files.createTempDirectory("dshackle-fs-test")
        def subdir1 = dir.resolve("subdir1")
        Files.createDirectory(subdir1)
        Files.write(subdir1.resolve("test1.txt"), "1".getBytes())
        Files.write(subdir1.resolve("test2.txt"), "2".getBytes())
        def subdir2 = dir.resolve("subdir2")
        Files.createDirectory(subdir2)
        Files.write(subdir2.resolve("test3.txt"), "3".getBytes())
        Files.write(subdir2.resolve("test4.txt"), "4".getBytes())
        Files.write(dir.resolve("test5.txt"), "5".getBytes())

        def publisher = new FilesStorageAccess.FilesPublisher(dir)
        when:
        def act = Flux.from(publisher)
                .collectList().block(Duration.ofSeconds(1))
        then:
        act.collect {dir.relativize(it).toString() }.toSorted() == [
                "test5.txt",
                "subdir1/test1.txt", "subdir1/test2.txt",
                "subdir2/test3.txt", "subdir2/test4.txt"
        ].toSorted()
    }

    def "Scan files with actual limit"() {
        setup:
        def dir = Files.createTempDirectory("dshackle-fs-test")
        def subdir = dir.resolve("subdir")
        Files.createDirectory(subdir)
        (10..99).forEach { i ->
            Files.write(subdir.resolve("test${i}.txt"), i.toString().getBytes())
        }

        def publisher = new FilesStorageAccess.FilesPublisher(dir)
        when:
        def act = Flux.from(publisher)
                .filter {
                    def i = it.fileName.toString().substring(4, 6).toInteger()
                    println("++++ i = $i / ${it.fileName.toString()} - ${it.fileName.toString().substring(4, 6)}")
                    i < 15
                }
                .doOnNext {
                    println("!!!!!!!!")
                    println("!!!!!!!! ACCEPT $it")
                    println("!!!!!!!!")
                }
                .doOnError { t ->
                    t.printStackTrace()
                }
                .collectList().block(Duration.ofSeconds(1))
        then:
        act.collect {dir.relativize(it).toString() }.toSorted() == [
                "subdir/test10.txt", "subdir/test11.txt", "subdir/test12.txt", "subdir/test13.txt", "subdir/test14.txt"
        ]
    }
}
