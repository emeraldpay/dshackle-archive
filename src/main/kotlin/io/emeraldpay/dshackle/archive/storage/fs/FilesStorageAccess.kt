package io.emeraldpay.dshackle.archive.storage.fs

import io.emeraldpay.dshackle.archive.config.RunConfig
import io.emeraldpay.dshackle.archive.storage.FilenameGenerator
import io.emeraldpay.dshackle.archive.storage.StorageAccess
import java.io.File
import java.io.IOException
import java.io.OutputStream
import java.nio.file.FileVisitResult
import java.nio.file.FileVisitor
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.attribute.BasicFileAttributes
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import org.apache.avro.file.SeekableFileInput
import org.apache.avro.file.SeekableInput
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
        @Autowired private val runConfig: RunConfig,
        @Autowired private val filenameGenerator: FilenameGenerator,
) : StorageAccess {

    companion object {
        private val log = LoggerFactory.getLogger(FilesStorageAccess::class.java)
    }

    private val dir = Path.of(runConfig.files.dir)

    init {
        Files.createDirectories(dir)
    }

    private val fileManipulationLock = ReentrantLock()
    private val okFiles = ConcurrentHashMap<Path, Boolean>()

    fun ensureFile(path: String, append: Boolean): Path {
        val fullPath = dir.resolve(path)
        if (okFiles.containsKey(fullPath)) {
            return fullPath
        }
        fileManipulationLock.withLock {
            val create: Boolean = if (!Files.exists(fullPath)) {
                Files.createDirectories(fullPath.parent)
                true
            } else if (!append) {
                Files.deleteIfExists(fullPath)
                true
            } else {
                false
            }
            if (create) {
                Files.createFile(fullPath)
            }
            okFiles.put(fullPath, true)
        }
        return fullPath
    }

    override fun listArchive(height: Long): Flux<String> {
        val dirs = listOf(
                "${filenameGenerator.parentDir}/${filenameGenerator.getLevel0(height)}"
        )
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
            files.forEach {
                Files.deleteIfExists(Path.of(it))
            }
        }.then()
    }

    override fun getURI(file: String): String {
        return File(file).absolutePath
    }

    override fun createWriter(path: String): OutputStream {
        val target = ensureFile(path, false)
        return Files.newOutputStream(target)
    }

    override fun createReader(path: String): SeekableInput {
        val fullPath = dir.resolve(path)
        return SeekableFileInput(fullPath.toFile())
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
            val done = AtomicBoolean(false)
            val shouldContinue = {
                if (!cancelled.get() && !done.get()) {
                    FileVisitResult.CONTINUE
                } else {
                    FileVisitResult.TERMINATE
                }
            }

            val visitor: FileVisitor<Path> = object : FileVisitor<Path> {
                override fun visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult {
                    s.onNext(file)
                    return shouldContinue()
                }

                override fun preVisitDirectory(dir: Path?, attrs: BasicFileAttributes?): FileVisitResult {
                    return shouldContinue()
                }

                override fun visitFileFailed(file: Path, exc: IOException): FileVisitResult {
                    if (file == start) {
                        if (exc is java.nio.file.NoSuchFileException) {
                            s.onComplete()
                            done.set(true)
                            return FileVisitResult.TERMINATE
                        }
                    }
                    done.set(true)
                    s.onError(exc)
                    return FileVisitResult.TERMINATE
                }

                override fun postVisitDirectory(dir: Path, exc: IOException?): FileVisitResult {
                    if (dir == start) {
                        s.onComplete()
                        done.set(true)
                        return FileVisitResult.TERMINATE
                    }
                    return shouldContinue()
                }
            }

            try {
                Files.walkFileTree(start, setOf(), 4, visitor)
                // Make sure the publisher is complete if for a some reason it didn't complete by the visitor.
                // Should never happen
                if (!done.get()) {
                    done.set(true)
                    s.onComplete()
                }
            } catch (t: Throwable) {
                // Must be already catch by the visitor, but checking here just in case
                if (!done.get()) {
                    s.onError(t)
                }
            }
        }
    }

}