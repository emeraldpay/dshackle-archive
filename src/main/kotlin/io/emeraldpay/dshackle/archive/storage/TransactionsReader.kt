package io.emeraldpay.dshackle.archive.storage

import com.linkedin.avro.fastserde.FastSpecificDatumReader
import io.emeraldpay.dshackle.archive.avro.Transaction
import org.apache.avro.file.DataFileReader
import org.apache.avro.file.SeekableInput
import org.apache.avro.specific.SpecificDatumReader
import org.reactivestreams.Publisher
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Repository
import java.nio.file.Path

@Repository
class TransactionsReader : AvroReader<Transaction> {

    companion object {
        private val log = LoggerFactory.getLogger(TransactionsReader::class.java)
    }

    override fun open(input: SeekableInput): Publisher<Transaction> {
        val datumReader = SpecificDatumReader(Transaction::class.java)
        val dataFileReader: DataFileReader<Transaction> = DataFileReader(input, datumReader)
        return AvroPublisher(dataFileReader)
    }

    @Deprecated("open with SeekableInput")
    fun open(file: Path): Publisher<Transaction> {
        log.info("Read transactions from ${file.fileName}")

        val datumReader = FastSpecificDatumReader<Transaction>(Transaction.getClassSchema())
        val dataFileReader: DataFileReader<Transaction> = DataFileReader<Transaction>(file.toFile(), datumReader)
        return AvroPublisher(dataFileReader)
    }
}
