package io.emeraldpay.dshackle.archive.storage.gcp

import com.google.api.gax.paging.Page
import com.google.cloud.storage.Blob
import com.google.cloud.storage.BlobId
import com.google.cloud.storage.BlobInfo
import com.google.cloud.storage.Storage
import io.emeraldpay.dshackle.archive.FileType
import io.emeraldpay.dshackle.archive.model.Chunk
import io.emeraldpay.dshackle.archive.storage.FilenameGenerator
import io.emeraldpay.dshackle.archive.storage.StorageAccess
import org.apache.avro.file.SeekableInput
import org.reactivestreams.Publisher
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import java.io.OutputStream
import java.nio.channels.Channels
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong

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
                prefix = path + filenameGenerator.getLevel0(height) + "/", // + filenameGenerator.getLevel1(height), // + "/",
                rangeStart = googleStorage.bucketPath + "/" + filenameGenerator.getIndividualFilename(FileType.BLOCKS.asTypeSingle(), height),
                rangeEnd = path + filenameGenerator.getLevel0(height) + "/" + filenameGenerator.maxLevelValue(), // stop at the max level value, before range-* starts
            ),
            ListQuery(
                path + filenameGenerator.getLevel0(height) + "/",
                googleStorage.bucketPath + "/" + filenameGenerator.getRangeFilename(FileType.BLOCKS.asTypeSingle(), Chunk(height, 0)),
            ),
        )
        log.info("Query lists for for: $query")
        return Flux.fromIterable(query)
            .flatMap { query(it) }
            .map {
                blockchainDir + it.blobId.name.substring(this.path.length)
            }
            .doOnNext {
                log.info("File: $it")
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

    override fun createWriter(path: String): OutputStream {
        val blobId = BlobId.of(googleStorage.bucket, googleStorage.getBucketPath(path))
        val blobInfo = BlobInfo.newBuilder(blobId).build()
        val channel = googleStorage.storage.writer(blobInfo, Storage.BlobWriteOption.disableGzipContent())
            ?: throw IllegalStateException("Blob ${blobId.toGsUtilUri()} cannot be created")

        return Channels.newOutputStream(channel)
    }

    override fun createReader(path: String): SeekableInput {
        val blobId = BlobId.of(googleStorage.bucket, googleStorage.getBucketPath(path))
        val blob = googleStorage.storage.get(blobId)
        val channel = googleStorage.storage.reader(blobId)
            ?: throw IllegalStateException("Blob ${blobId.toGsUtilUri()} cannot be created")
        return SeekableChannelInput(blob.size, channel)
    }

    data class ListQuery(
        val prefix: String,
        val rangeStart: String,
        val rangeEnd: String? = null,
    )

    class BlobsPublisher(
        private val storage: Storage,
        private val bucket: String,
        private val path: String,
        private val rangeStart: String?,
        private val rangeEnd: String?,
    ) : Publisher<BlobInfo> {

        private var currentPage: Page<Blob>? = null
        private var index: Int = 0

        override fun subscribe(s: Subscriber<in BlobInfo>) {
            val cancelled = AtomicBoolean(false)
            val limit = AtomicLong(0)
            s.onSubscribe(
                object : Subscription {
                    override fun request(n: Long) {
                        limit.set(n)
                        scan(cancelled, limit, s)
                    }

                    override fun cancel() {
                        cancelled.set(true)
                    }
                },
            )
        }

        fun scan(cancelled: AtomicBoolean, limit: AtomicLong, s: Subscriber<in Blob>) {
            if (currentPage == null) {
                val opts: List<Storage.BlobListOption> = listOf(
                    Storage.BlobListOption.prefix(path),
                    Storage.BlobListOption.pageSize(100),
                ).let {
                    if (rangeStart != null) {
                        it + listOf(Storage.BlobListOption.startOffset(rangeStart))
                    } else {
                        it
                    }
                }.let {
                    if (rangeEnd != null) {
                        it + listOf(Storage.BlobListOption.endOffset(rangeEnd))
                    } else {
                        it
                    }
                }
                currentPage = storage.list(bucket, *opts.toTypedArray())
            }

            while (!cancelled.get() && limit.get() > 0) {
                val pageItems = currentPage!!.values.toList()
                while (limit.get() > 0 && pageItems.size > index) {
                    val next = pageItems[index]
                    index += 1
                    s.onNext(next)
                    limit.decrementAndGet()
                }
                val consumed = index >= pageItems.size
                if (consumed) {
                    if (!currentPage!!.hasNextPage()) {
                        s.onComplete()
                        cancelled.set(true)
                    } else {
                        // makes a request for a next page
                        currentPage = currentPage!!.nextPage
                        index = 0
                    }
                }
            }
        }
    }
}
