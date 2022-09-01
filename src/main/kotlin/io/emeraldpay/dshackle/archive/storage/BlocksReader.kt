package io.emeraldpay.dshackle.archive.storage

import io.emeraldpay.dshackle.archive.avro.Block
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicBoolean
import org.apache.avro.file.DataFileReader
import org.apache.avro.file.FileReader
import org.apache.avro.file.SeekableFileInput
import org.apache.avro.file.SeekableInput
import org.apache.avro.specific.SpecificDatumReader
import org.reactivestreams.Publisher
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Repository

@Repository
class BlocksReader: AvroReader<Block> {

    companion object {
        private val log = LoggerFactory.getLogger(BlocksReader::class.java)
    }

    fun open(file: Path): Publisher<Block> {
        log.info("Read blocks from ${file.fileName}")
        return open(SeekableFileInput(file.toFile()))
    }

    override fun open(input: SeekableInput): Publisher<Block> {
        val datumReader = SpecificDatumReader<Block>(Block::class.java)
        val dataFileReader: DataFileReader<Block> = DataFileReader<Block>(input, datumReader)
        return AvroPublisher(dataFileReader)
    }

}