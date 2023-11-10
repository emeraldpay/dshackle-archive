package io.emeraldpay.dshackle.archive.storage.s3

import org.reactivestreams.Publisher
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription
import org.slf4j.LoggerFactory
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response
import software.amazon.awssdk.services.s3.model.S3Object
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong

class S3Publisher(
    val storage: S3Client,
    private val bucket: String,
    private val path: String,
    private val rangeStart: String?,
    private val rangeEnd: String?,
) : Publisher<S3Object> {

    companion object {
        private val log = LoggerFactory.getLogger(S3Publisher::class.java)
    }

    private var currentPage: ListObjectsV2Response? = null
    private var index: Int = 0

    override fun subscribe(s: Subscriber<in S3Object>) {
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

    fun makeRequest(token: String? = null): ListObjectsV2Request {
        return ListObjectsV2Request.builder()
            .bucket(bucket)
            .prefix(path)
            .maxKeys(100)
            .let {
                if (rangeStart != null) {
                    it.startAfter(rangeStart)
                } else {
                    it
                }
            }
            .let {
                if (token != null) {
                    it.continuationToken(token)
                } else {
                    it
                }
            }
            .build()
    }

    fun isFinal(obj: S3Object): Boolean {
        return rangeEnd != null && obj.key() >= rangeEnd
    }

    fun scan(cancelled: AtomicBoolean, limit: AtomicLong, s: Subscriber<in S3Object>) {
        if (currentPage == null) {
            currentPage = storage.listObjectsV2(makeRequest())
        }

        while (!cancelled.get() && limit.get() > 0) {
            val pageItems = currentPage!!.contents()
            while (limit.get() > 0 && pageItems.size > index) {
                val next = pageItems[index]
                index += 1
                if (isFinal(next)) {
                    s.onComplete()
                    cancelled.set(true)
                    return
                }
                s.onNext(next)
                limit.decrementAndGet()
            }
            val consumed = index >= pageItems.size
            if (consumed) {
                if (!currentPage!!.isTruncated || currentPage!!.nextContinuationToken() == null) {
                    s.onComplete()
                    cancelled.set(true)
                } else {
                    // makes a request for a next page
                    currentPage = storage.listObjectsV2(makeRequest(currentPage!!.nextContinuationToken()))
                    index = 0
                }
            }
        }
    }
}
