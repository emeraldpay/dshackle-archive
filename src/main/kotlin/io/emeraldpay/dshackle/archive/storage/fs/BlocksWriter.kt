package io.emeraldpay.dshackle.archive.storage.fs

import io.emeraldpay.dshackle.archive.config.RunConfig
import io.emeraldpay.dshackle.archive.avro.Block
import io.emeraldpay.dshackle.archive.BlocksRange
import io.emeraldpay.dshackle.archive.storage.BlockDetails
import io.emeraldpay.dshackle.archive.storage.ConfiguredFilenameGenerator
import io.emeraldpay.dshackle.archive.storage.CurrentStorage
import java.nio.file.Path
import java.time.Instant
import org.apache.avro.AvroRuntimeException
import org.apache.avro.file.CodecFactory
import org.apache.avro.file.DataFileWriter
import org.apache.avro.specific.SpecificDatumWriter
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Repository

@Repository
class BlocksWriter(
        @Autowired private val configuredFilenameGenerator: ConfiguredFilenameGenerator,
        @Autowired private val runConfig: RunConfig,
) {

    companion object {
        private val log = LoggerFactory.getLogger(BlocksWriter::class.java)
    }

    private val currentWriters = CurrentStorage(100)

    fun open(chunk: BlocksRange.Chunk): BlocksFileAccess {
        val file = configuredFilenameGenerator.fileFor("blocks", chunk, false)
        log.info("Save blocks to ${file.fileName}")
        return open(file)
    }

    fun open(file: Path): BlocksFileAccess {
        return currentWriters.get(file) {
            val dataFileWriter: DataFileWriter<Block>
            val datumWriter = SpecificDatumWriter<Block>(Block::class.java)
            dataFileWriter = DataFileWriter<Block>(datumWriter)
            dataFileWriter.setCodec(CodecFactory.snappyCodec())
            LocalFileAccess(dataFileWriter.create(Block.getClassSchema(), file.toFile()), runConfig, file, currentWriters)
        }
    }

    fun closeAll() {
        currentWriters.close()
    }

    interface BlocksFileAccess : AutoCloseable {
        fun append(block: BlockDetails)
        fun append(block: Block)
    }

    class LocalFileAccess(
            dataFileWriter: DataFileWriter<Block>,
            private val runConfig: RunConfig,
            path: Path,
            currentStorage: CurrentStorage,
    ) : BlocksFileAccess, AutoCloseable, BaseAvroWriter<Block>(dataFileWriter, path, currentStorage) {

        override fun append(block: BlockDetails) {
            val datum = Block().apply {
                blockchainType = runConfig.chainType
                blockchainId = runConfig.getChainId()
                archiveTimestamp = Instant.now()

                timestamp = block.timestamp
                blockId = block.hash
                height = block.height
                parentId = block.parentHash
                json = block.raw
                if (block.uncles != null) {
                    unclesCount = block.uncles.size
                    if (block.uncles.isNotEmpty()) {
                        uncle0Json = block.uncles[0]
                    }
                    if (block.uncles.size > 1) {
                        uncle1Json = block.uncles[1]
                    }
                }
            }
            this.append(datum)
        }

        override fun append(datum: Block) {
            try {
                dataFileWriter.append(datum)
            } catch (t: AvroRuntimeException) {
                log.error("Failed to write block: ${datum.height}. Error ${t.javaClass} ${t.message}")
                drop()
                throw t
            }
        }

    }

}