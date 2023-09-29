package io.emeraldpay.dshackle.archive.storage.gcp

import com.google.api.gax.paging.Page
import com.google.cloud.storage.Blob
import com.google.cloud.storage.BlobInfo
import com.google.cloud.storage.Storage
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import org.reactivestreams.Publisher
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription

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
