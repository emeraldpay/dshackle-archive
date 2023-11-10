package io.emeraldpay.dshackle.archive.storage.gcp

import com.google.cloud.storage.Blob
import com.google.cloud.storage.BlobId
import com.google.cloud.storage.BlobInfo
import com.google.cloud.storage.Storage
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
import java.io.OutputStream
import java.net.URI
import java.nio.channels.Channels
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.io.path.name
import kotlin.io.path.pathString

@Service
@Profile("with-gcp")
open class GSStorageAccess(
    @Autowired private val filenameGenerator: FilenameGenerator,
    @Autowired private val googleStorage: GoogleStorage,
) : StorageAccess {

    companion object {
        private val log = LoggerFactory.getLogger(GSStorageAccess::class.java)
    }

    private val blockchainDir = filenameGenerator.parentDir
    private val path = googleStorage.bucketPath + "/" + blockchainDir

    override fun getDirBlockSizeL1(): Long {
        return filenameGenerator.dirBlockSizeL1
    }

    override fun listArchiveLevel0(height: Long): Flux<String> {
        // use separate filters for Stream files and Ranges,
        // it stops processing stream files when last block is reached and starts processing ranges form given height
        val query = listOf(
            ListQuery(
                prefix = path + filenameGenerator.getLevel0(height) + "/",
                rangeStart = googleStorage.bucketPath + "/" + filenameGenerator.getIndividualFilename(FileType.BLOCKS.asTypeSingle(), height),
                rangeEnd = path + filenameGenerator.getLevel0(height) + "/" + filenameGenerator.maxLevelValue(), // stop at the max level value, before range-* starts
            ),
            ListQuery(
                path + filenameGenerator.getLevel0(height) + "/",
                googleStorage.bucketPath + "/" + filenameGenerator.getRangeFilename(FileType.BLOCKS.asTypeSingle(), Chunk(height, 0)),
            ),
        )
        log.debug("Query lists for for: {}", query)
        return Flux.fromIterable(query)
            .flatMap { query(it) }
            .map {
                blockchainDir + it.blobId.name.substring(this.path.length)
            }
    }

    fun query(query: ListQuery): Flux<BlobInfo> =
        Flux.from(BlobsPublisher(googleStorage.storage, googleStorage.bucket, query.prefix, query.rangeStart, query.rangeEnd))

    override fun deleteArchives(files: List<String>): Mono<Void> {
        return Flux.fromIterable(files)
            .map {
                BlobId.of(googleStorage.bucket, googleStorage.getBucketPath(it))
            }
            .collectList()
            .flatMap {
                Mono.fromCallable {
                    googleStorage.storage.delete(it)
                }.subscribeOn(Schedulers.boundedElastic())
            }
            .then()
    }

    override fun getURI(file: String): String {
        return "gs://${googleStorage.bucket}/${googleStorage.getBucketPath(file)}"
    }

    override fun exists(path: String): Boolean {
        val blobId = BlobId.of(googleStorage.bucket, googleStorage.getBucketPath(path))
        val blobInfo = BlobInfo.newBuilder(blobId)
            .build()
        return googleStorage.storage.get(blobInfo.blobId) != null
    }

    override fun createWriter(path: String): OutputStream {
        val blobId = BlobId.of(googleStorage.bucket, googleStorage.getBucketPath(path))
        val blobInfo = BlobInfo.newBuilder(blobId).build()
        val channel = googleStorage.storage.writer(blobInfo, Storage.BlobWriteOption.disableGzipContent())
            ?: throw IllegalStateException("Blob ${blobId.toGsUtilUri()} cannot be created")

        return Channels.newOutputStream(channel)
    }

    override fun createReader(fullPath: Path): SeekableInput {
        val blobId = BlobId.of(googleStorage.bucket, fullPath.pathString.removePrefix("/"))
        return createReader(blobId)
    }

    override fun createReader(path: String): SeekableInput {
        val blobId = BlobId.of(googleStorage.bucket, googleStorage.getBucketPath(path))
        return createReader(blobId)
    }

    private fun createReader(blobId: BlobId): SeekableChannelInput {
        val blob = googleStorage.storage.get(blobId) ?: throw IllegalStateException("Blob ${blobId.toGsUtilUri()} not found")
        val channel = googleStorage.storage.reader(blobId)
            ?: throw IllegalStateException("Blob ${blobId.toGsUtilUri()} cannot be created")
        return SeekableChannelInput(blob.size, channel)
    }

    override fun inputSources(patterns: List<String>, range: Chunk): StorageAccess.InputSources {
        var transactions = Flux.empty<Path>()
        var blocks = Flux.empty<Path>()
        for (pattern in patterns) {
            if (StringUtils.containsAny(pattern, "?*")) {
                throw IllegalArgumentException(
                    "Patterns are not supported, input sources should contain prefixes only",
                )
            }
            val blobPrefix = BlobId.fromGsUtilUri(pattern)
            if (blobPrefix.bucket != googleStorage.bucket) {
                throw IllegalArgumentException(
                    "Different source and target buckets are not currently supported (${blobPrefix.bucket} != ${googleStorage.bucket})",
                )
            }
            val blobs: Iterable<Blob> = googleStorage.storage
                .list(blobPrefix.bucket, Storage.BlobListOption.prefix(blobPrefix.name))
                .iterateAll()
            val blobFlux = Flux.fromIterable(blobs)
                .map { blob -> blobLink(blob) }
                .filter { file ->
                    val chunk = filenameGenerator.parseRange(file.name)
                    if (chunk == null) {
                        log.debug("Skip no chunk ${file.name}")
                    }
                    val accept = chunk != null && range.intersects(chunk)
                    if (!accept) {
                        log.trace("Skip diff chunk ${file.name}")
                    }
                    accept
                }
                .share()
            transactions = transactions.concatWith(
                blobFlux.filter {
                    it.name.endsWith(".txes.avro") || it.name.endsWith(".transactions.avro")
                },
            )
            blocks = blocks.concatWith(
                blobFlux.filter {
                    it.name.endsWith(".block.avro") || it.name.endsWith(".blocks.avro")
                },
            )
        }
        return StorageAccess.InputSources(transactions, blocks)
    }

    private fun blobLink(blob: Blob): Path {
        if (blob.isDirectory) {
            throw IllegalStateException("storage.list shouldn't list directories")
        }
        val uri = blob.blobId.toGsUtilUri()
        return Paths.get(URI.create(uri))
    }
}
