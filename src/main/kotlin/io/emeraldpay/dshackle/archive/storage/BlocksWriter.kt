package io.emeraldpay.dshackle.archive.storage

import io.emeraldpay.dshackle.archive.config.RunConfig
import io.emeraldpay.dshackle.archive.avro.Block
import io.emeraldpay.dshackle.archive.BlocksRange
import io.emeraldpay.dshackle.archive.FileType
import java.nio.file.Path
import java.time.Instant
import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
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
        @Autowired private val targetStorage: TargetStorage,
) {

    companion object {
        private val log = LoggerFactory.getLogger(BlocksWriter::class.java)
    }

    private val currentWriters = CurrentStorage(100)

    fun open(chunk: BlocksRange.Chunk): BlocksFileAccess {
        val file = configuredFilenameGenerator.fileFor(FileType.BLOCKS, chunk)
        return open(file)
    }

    fun open(file: String): BlocksFileAccess {
        return currentWriters.get(file) {
            log.info("Save blocks to $file")
            val dataFileWriter: DataFileWriter<Block>
            val datumWriter = SpecificDatumWriter<Block>(Block::class.java)
            dataFileWriter = DataFileWriter<Block>(datumWriter)
            dataFileWriter.setCodec(CodecFactory.snappyCodec())
            val outputStream = targetStorage.current.createWriter(file)
            LocalFileAccess(dataFileWriter.create(Block.getClassSchema(), outputStream), runConfig, file, currentWriters, targetStorage.current)
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
            path: String,
            currentStorage: CurrentStorage,
            access: StorageAccess,
    ) : BlocksFileAccess, AutoCloseable, BaseAvroWriter<Block>(dataFileWriter, path, currentStorage, access) {

        private val writeLock = ReentrantLock()

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
                writeLock.withLock {
                    dataFileWriter.append(datum)
                }
            } catch (t: AvroRuntimeException) {
                log.error("Failed to write block: ${datum.height}. Error ${t.javaClass} ${t.message}")
                drop()
                throw t
            }
        }

    }

}