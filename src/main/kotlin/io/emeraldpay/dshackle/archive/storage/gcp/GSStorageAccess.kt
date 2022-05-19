package io.emeraldpay.dshackle.archive.storage.gcp

import com.google.api.gax.paging.Page
import com.google.cloud.storage.Blob
import com.google.cloud.storage.BlobId
import com.google.cloud.storage.Storage
import io.emeraldpay.dshackle.archive.config.RunConfig
import io.emeraldpay.dshackle.archive.storage.FilenameGenerator
import io.emeraldpay.dshackle.archive.storage.StorageAccess
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
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

@Service
@Profile("with-gcp")
class GSStorageAccess(
        @Autowired private val filenameGenerator: FilenameGenerator,
        @Autowired private val googleStorage: GoogleStorage,
) : StorageAccess {

    companion object {
        private val log = LoggerFactory.getLogger(GSStorageAccess::class.java)
    }

    private val blockchainDir = filenameGenerator.parentDir
    private val path = googleStorage.bucketPath + "/" + blockchainDir

    override fun listArchive(height: List<Long>?): Flux<String> {
        val prefixes = if (height == null) {
            listOf(path)
        } else {
            height.map(filenameGenerator::getLevel0)
                    .toSet()
                    .map { "$path$it" }
        }
        return Flux.fromIterable(prefixes)
                .flatMap { prefix ->
                    Flux.from(BlobsPublisher(googleStorage.storage, googleStorage.bucket, prefix))
                }
                .map {
                    it.blobId.name.substring(this.path.length)
                }
    }

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

    override fun locationFor(file: String): String {
        return "gs://${googleStorage.bucket}/${googleStorage.getBucketPath(file)}"
    }

    class BlobsPublisher(
            private val storage: Storage,
            private val bucket: String,
            private val path: String,
    ) : Publisher<Blob> {

        private var currentPage: Page<Blob>? = null
        private var index: Int = 0

        override fun subscribe(s: Subscriber<in Blob>) {
            val cancelled = AtomicBoolean(false)
            val limit = AtomicLong(0)
            s.onSubscribe(object : Subscription {
                override fun request(n: Long) {
                    limit.set(n)
                    scan(cancelled, limit, s)
                }

                override fun cancel() {
                    cancelled.set(true)
                }
            })
        }

        fun scan(cancelled: AtomicBoolean, limit: AtomicLong, s: Subscriber<in Blob>) {
            if (currentPage == null) {
                currentPage = storage.list(bucket, Storage.BlobListOption.prefix(path))
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
                        currentPage!!.nextPage
                        index = 0
                    }
                }
            }
        }
    }

}