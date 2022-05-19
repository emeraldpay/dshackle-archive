package io.emeraldpay.dshackle.archive.storage.fs

import io.emeraldpay.dshackle.archive.storage.FilenameGenerator
import io.emeraldpay.dshackle.archive.storage.StorageAccess
import java.io.File
import java.io.IOException
import java.nio.file.FileVisitResult
import java.nio.file.FileVisitor
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.attribute.BasicFileAttributes
import java.util.concurrent.atomic.AtomicBoolean
import org.reactivestreams.Publisher
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

@Service
class FilesStorageAccess(
        @Autowired private val filenameGenerator: FilenameGenerator,
) : StorageAccess {

    companion object {
        private val log = LoggerFactory.getLogger(FilesStorageAccess::class.java)
    }

    override fun listArchive(height: List<Long>?): Flux<String> {
        val dirs = if (height == null || height.isEmpty()) {
            listOf(filenameGenerator.parentDir)
        } else {
            height.map(filenameGenerator::getLevel0)
                    .toSet()
                    .map {
                        listOf(filenameGenerator.parentDir, it).joinToString("/")
                    }
        }
        val parent = Path.of(filenameGenerator.parentDir)

        return Flux.fromIterable(dirs)
                .flatMap { dir ->
                    Flux.from(FilesPublisher(Path.of(dir)))
                }.map {
                    parent.relativize(it).toString()
                }
    }

    override fun deleteArchives(files: List<String>): Mono<Void> {
        return Mono.fromCallable {
            println("delete files $files")
            files.forEach {
                Files.deleteIfExists(Path.of(it))
            }
        }.then()
    }

    override fun locationFor(file: String): String {
        return File(file).absolutePath
    }

    class FilesPublisher(
            private val start: Path,
    ) : Publisher<Path> {

        override fun subscribe(s: Subscriber<in Path>) {
            val cancelled = AtomicBoolean(false)
            s.onSubscribe(object : Subscription {
                override fun request(n: Long) {
                    scan(cancelled, s)
                }

                override fun cancel() {
                    cancelled.set(true)
                }
            })
        }

        fun scan(cancelled: AtomicBoolean, s: Subscriber<in Path>) {
            val visitor: FileVisitor<Path> = object : FileVisitor<Path> {
                override fun visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult {
                    s.onNext(file)
                    return if (!cancelled.get()) {
                        FileVisitResult.CONTINUE
                    } else {
                        FileVisitResult.TERMINATE
                    }
                }

                override fun preVisitDirectory(dir: Path?, attrs: BasicFileAttributes?): FileVisitResult {
                    return if (!cancelled.get()) {
                        FileVisitResult.CONTINUE
                    } else {
                        FileVisitResult.TERMINATE
                    }
                }

                override fun visitFileFailed(file: Path?, exc: IOException?): FileVisitResult {
                    return if (!cancelled.get()) {
                        FileVisitResult.CONTINUE
                    } else {
                        FileVisitResult.TERMINATE
                    }
                }

                override fun postVisitDirectory(dir: Path?, exc: IOException?): FileVisitResult {
                    return if (!cancelled.get()) {
                        FileVisitResult.CONTINUE
                    } else {
                        FileVisitResult.TERMINATE
                    }
                }
            }

            Files.walkFileTree(start, setOf(), 4, visitor)
        }
    }

}