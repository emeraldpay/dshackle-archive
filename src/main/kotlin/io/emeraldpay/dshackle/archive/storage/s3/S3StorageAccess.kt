package io.emeraldpay.dshackle.archive.storage.s3

import io.emeraldpay.dshackle.archive.FileType
import io.emeraldpay.dshackle.archive.model.Chunk
import io.emeraldpay.dshackle.archive.storage.FilenameGenerator
import io.emeraldpay.dshackle.archive.storage.ListQuery
import io.emeraldpay.dshackle.archive.storage.StorageAccess
import org.apache.avro.file.SeekableInput
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.model.Delete
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.HeadObjectRequest
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request
import software.amazon.awssdk.services.s3.model.ObjectIdentifier
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import software.amazon.awssdk.services.s3.model.S3Exception
import software.amazon.awssdk.services.s3.model.S3Object
import java.io.OutputStream
import java.net.URI
import java.nio.channels.Channels
import java.nio.channels.Pipe
import java.nio.file.Path
import java.util.concurrent.Executors
import kotlin.io.path.name
import kotlin.io.path.pathString

@Service
@Profile("with-s3")
open class S3StorageAccess(
    @Autowired private val filenameGenerator: FilenameGenerator,
    @Autowired private val s3Config: S3Config,
) : StorageAccess {

    companion object {
        private val log = LoggerFactory.getLogger(S3StorageAccess::class.java)
    }

    private val threads = Executors.newCachedThreadPool()
    private val blockchainDir = filenameGenerator.parentDir
    private val path = s3Config.bucketPath.forBlockchain(blockchainDir)

    override fun listArchiveLevel0(height: Long): Flux<String> {
        // use separate filters for Stream files and Ranges,
        // it stops processing stream files when last block is reached and starts processing ranges form given height
        val query = listOf(
            ListQuery(
                prefix = path.fullPathFor(filenameGenerator.getLevel0(height) + "/"),
                rangeStart = s3Config.bucketPath.fullPathFor(filenameGenerator.getIndividualFilename(FileType.BLOCKS.asTypeSingle(), height)),
                rangeEnd = path.fullPathFor(filenameGenerator.getLevel0(height) + "/" + filenameGenerator.maxLevelValue()), // stop at the max level value, before range-* starts
            ),
            ListQuery(
                path.fullPathFor(filenameGenerator.getLevel0(height) + "/"),
                s3Config.bucketPath.fullPathFor(filenameGenerator.getRangeFilename(FileType.BLOCKS.asTypeSingle(), Chunk(height, 0))),
            ),
        )
        log.debug("Query lists for for: {}", query)
        return Flux.fromIterable(query)
            .flatMap { query(it) }
            .map {
                blockchainDir + it.key().substring(this.path.fullPathFor("").length)
            }
    }

    fun query(query: ListQuery): Flux<S3Object> =
        Flux.from(S3Publisher(s3Config.storage, s3Config.bucket, query.prefix, query.rangeStart, query.rangeEnd))

    override fun deleteArchives(files: List<String>): Mono<Void> {
        return Mono.fromCallable {
            val request = DeleteObjectsRequest.builder()
                .bucket(s3Config.bucket)
                .delete(
                    Delete.builder().objects(
                        files.map { ObjectIdentifier.builder().key(it).build() },
                    ).build(),
                )
                .build()
            s3Config.storage.deleteObjects(request)
        }.subscribeOn(Schedulers.boundedElastic()).then()
    }

    override fun getURI(file: String): String {
        return "s3://${s3Config.bucket}/${s3Config.bucketPath.fullPathFor(file)}"
    }

    fun getBucket(uri: URI): String {
        val host = uri.host
        return if (host.contains(".")) {
            host.substring(0, host.indexOf("."))
        } else {
            host
        }
    }

    fun getKey(uri: URI): String {
        return uri.path.substring(1)
    }

    override fun exists(path: String): Boolean {
        try {
            val request = HeadObjectRequest.builder()
                .bucket(s3Config.bucket)
                .key(s3Config.bucketPath.fullPathFor(path))
                .build()
            val response = s3Config.storage.headObject(request)
            return response != null
        } catch (e: S3Exception) {
            if (e.statusCode() == 404) {
                return false
            }
            throw e
        }
    }

    override fun createWriter(path: String): OutputStream {
        // S3 client needs an InputStream, so here are the Pipes
        // multiple reading/writing threads are not properly supported by PipedOutputStream https://bugs.openjdk.org/browse/JDK-7016956
        // so channels are used instead
        val pipe = Pipe.open()
        val output = Channels.newOutputStream(pipe.sink())
        val input = Channels.newInputStream(pipe.source())
        // because after making a request it locks the input stream there is no way to continue with writing to it,
        // so we need to run it in a separate thread
        threads.execute {
            try {
                val request = PutObjectRequest.builder()
                    .bucket(s3Config.bucket)
                    .key(s3Config.bucketPath.fullPathFor(path))
                    .build()
                val body = RequestBody.fromContentProvider({ input }, "application/avro")
                s3Config.storage.putObject(request, body)
                log.debug("S3 storage putObject done: $path")
            } catch (t: Throwable) {
                log.warn("Failed to write to S3: $path", t)
            } finally {
                try {
                    output.close()
                    input.close()
                } catch (t: Throwable) {
                    log.warn("Failed to close S3 writer", t)
                }
            }
        }
        return output
    }

    override fun createReader(path: String): SeekableInput {
        val request = GetObjectRequest.builder()
            .bucket(s3Config.bucket)
            .key(s3Config.bucketPath.fullPathFor(path))
            .build()
        val obj = s3Config.storage.getObject(request)
        return SeekableS3Object(obj)
    }

    override fun createReader(fullPath: Path): SeekableInput {
        return createReader(fullPath.pathString.removePrefix("/"))
    }

    override fun getDirBlockSizeL1(): Long {
        return filenameGenerator.dirBlockSizeL1
    }

    override fun inputSources(patterns: List<String>, range: Chunk): StorageAccess.InputSources {
        val allFiles = Flux.fromIterable(patterns)
            .flatMap {
                if (StringUtils.containsAny(it, "?*")) {
                    Mono.error(
                        IllegalArgumentException(
                            "Patterns are not supported, input sources should contain prefixes only: $patterns",
                        ),
                    )
                } else {
                    Mono.just(it)
                }
            }
            .flatMap(::listFiles, 1)
            .flatMap {
                val chunk = filenameGenerator.parseRange(it.key())
                if (chunk == null) {
                    log.debug("Skip no chunk ${it.key()}")
                    Mono.empty()
                } else if (!range.intersects(chunk)) {
                    log.trace("Skip diff chunk ${it.key()}")
                    Mono.empty()
                } else {
                    Mono.just(it)
                }
            }
            .map { Path.of(URI("s3://${s3Config.bucket}/${it.key()}")) }
            .share()

        val transactions = allFiles
            .filter {
                it.name.endsWith(".txes.avro") || it.name.endsWith(".transactions.avro")
            }
        val blocks = allFiles
            .filter {
                it.name.endsWith(".block.avro") || it.name.endsWith(".blocks.avro")
            }

        return StorageAccess.InputSources(transactions, blocks)
    }

    fun listFiles(pattern: String): Flux<S3Object> {
        val uri = URI(pattern)
        val bucket = getBucket(uri)
        if (bucket != s3Config.bucket) {
            throw IllegalArgumentException(
                "Different source and target buckets are not currently supported ($bucket != ${s3Config.bucket})",
            )
        }
        val request = ListObjectsV2Request.builder()
            .bucket(bucket)
            .prefix(getKey(uri))
            .build()
        return drain(request)
    }

    fun drain(request: ListObjectsV2Request): Flux<S3Object> {
        val resp = s3Config.storage.listObjectsV2(request)
        return if (resp.isTruncated) {
            Flux.fromIterable(resp.contents()).concatWith(
                Mono.fromCallable {
                    drain(request.toBuilder().continuationToken(resp.continuationToken()).build())
                }.flatMapMany { it },
            )
        } else {
            Flux.fromIterable(resp.contents())
        }
    }
}
