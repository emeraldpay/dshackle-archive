package io.emeraldpay.dshackle.archive.storage

import io.emeraldpay.dshackle.archive.avro.Block
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicBoolean
import org.apache.avro.file.DataFileReader
import org.apache.avro.file.FileReader
import org.apache.avro.specific.SpecificDatumReader
import org.reactivestreams.Publisher
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Repository

@Repository
class BlocksReader {

    companion object {
        private val log = LoggerFactory.getLogger(BlocksReader::class.java)
    }

    fun open(file: Path): Publisher<Block> {
        log.info("Read blocks from ${file.fileName}")

        val datumReader = SpecificDatumReader<Block>(Block::class.java)
        val dataFileReader: DataFileReader<Block> = DataFileReader<Block>(file.toFile(), datumReader)
        return LocalFileAccess(dataFileReader)
    }

    class LocalFileAccess(
            private val dataFileReader: FileReader<Block>
    ) : AutoCloseable, Publisher<Block> {

        override fun close() {
            try {
                dataFileReader.close()
            } catch (t: Throwable) {
                log.warn("Failed to close properly", t)
            }
        }

        override fun subscribe(s: Subscriber<in Block>) {
            val cancelled = AtomicBoolean(false)
            val subscriber = object : Subscription {
                override fun request(n: Long) {
                    cancelled.set(false)
                    (0 until n).forEach {
                        if (!cancelled.get()) {
                            next(s)
                        }
                    }
                }

                override fun cancel() {
                    cancelled.set(true)
                }
            }
            s.onSubscribe(subscriber)
        }

        fun next(s: Subscriber<in Block>) {
            if (!dataFileReader.hasNext()) {
                s.onComplete()
                close()
                return
            }
            try {
                val value = dataFileReader.next(null)
                if (value == null) {
                    log.warn("Source file ended unexpectedly")
                    s.onComplete()
                    close()
                    return
                }
                s.onNext(value)
            } catch (t: Throwable) {
                log.error("Failed to read next element", t)
                close()
                s.onError(t)
            }
        }

    }
}