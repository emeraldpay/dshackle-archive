package io.emeraldpay.dshackle.archive.storage

import org.apache.avro.file.FileReader
import org.reactivestreams.Publisher
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription
import org.slf4j.LoggerFactory
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong

class AvroPublisher<T>(
    private val dataFileReader: FileReader<T>,
) : Publisher<T> {

    companion object {
        private val log = LoggerFactory.getLogger(AvroPublisher::class.java)
    }

    // / use individual thread because otherwise it blocks while calling onNext
    private val thread = Executors.newFixedThreadPool(1)

    private fun close() {
        thread.shutdown()
        try {
            dataFileReader.close()
        } catch (t: Throwable) {
            log.warn("Failed to close properly", t)
        }
    }

    private fun doComplete(subscriber: Subscriber<in T>, read: AtomicBoolean) {
        close()
        subscriber.onComplete()
        read.set(true)
    }

    override fun subscribe(s: Subscriber<in T>) {
        val read = AtomicBoolean(false)
        val limit = AtomicLong(0)

        val subscriber = object : Subscription {
            override fun request(n: Long) {
                limit.set(n)
                readAll(limit, s, read)
            }

            override fun cancel() {
                limit.set(0)
            }
        }
        s.onSubscribe(subscriber)
    }

    fun readAll(limit: AtomicLong, subscriber: Subscriber<in T>, read: AtomicBoolean) {
        thread.execute {
            while (limit.get() > 0 && !read.get()) {
                limit.decrementAndGet()
                readNext(subscriber, read)
            }
        }
    }

    private fun readNext(subscriber: Subscriber<in T>, read: AtomicBoolean) {
        try {
            if (!dataFileReader.hasNext()) {
                doComplete(subscriber, read)
                return
            }
            val value = dataFileReader.next(null)
            if (value == null) {
                log.warn("Source file ended unexpectedly")
                doComplete(subscriber, read)
                return
            }
            subscriber.onNext(value)
        } catch (t: Throwable) {
            log.error("Failed to read next element", t)
            close()
            subscriber.onError(t)
        }
    }
}
