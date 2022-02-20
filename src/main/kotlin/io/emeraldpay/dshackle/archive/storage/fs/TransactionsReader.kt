package io.emeraldpay.dshackle.archive.storage.fs

import com.linkedin.avro.fastserde.FastSpecificDatumReader
import io.emeraldpay.dshackle.archive.avro.Transaction
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
class TransactionsReader {

    companion object {
        private val log = LoggerFactory.getLogger(TransactionsReader::class.java)
    }

    fun open(file: Path): Publisher<Transaction> {
        log.info("Read transactions from ${file.fileName}")

        val datumReader = SpecificDatumReader<Transaction>(Transaction::class.java)
        val dataFileReader: DataFileReader<Transaction> = DataFileReader<Transaction>(file.toFile(), datumReader)
        return LocalFileAccess(dataFileReader)
    }

    fun openFast(file: Path): Publisher<Transaction> {
        log.info("Read transactions from ${file.fileName}")

        val datumReader = FastSpecificDatumReader<Transaction>(Transaction.getClassSchema())
        val dataFileReader: DataFileReader<Transaction> = DataFileReader<Transaction>(file.toFile(), datumReader)
        return LocalFileAccess(dataFileReader)
    }

    class LocalFileAccess(
            private val dataFileReader: FileReader<Transaction>
    ) : AutoCloseable, Publisher<Transaction> {

        override fun close() {
            try {
                dataFileReader.close()
            } catch (t: Throwable) {
                log.warn("Failed to close properly", t)
            }
        }

        override fun subscribe(s: Subscriber<in Transaction>) {
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

        fun next(s: Subscriber<in Transaction>) {
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