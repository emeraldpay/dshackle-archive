package io.emeraldpay.dshackle.archive.storage

import io.emeraldpay.dshackle.archive.FileType
import io.emeraldpay.dshackle.archive.avro.Block
import io.emeraldpay.dshackle.archive.config.RunConfig
import io.emeraldpay.dshackle.archive.model.Chunk
import org.apache.avro.AvroRuntimeException
import org.apache.avro.file.CodecFactory
import org.apache.avro.file.DataFileWriter
import org.apache.avro.specific.SpecificDatumWriter
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Repository
import java.time.Instant
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

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

    fun openShared(chunk: Chunk): BlocksFileAccess {
        val file = configuredFilenameGenerator.fileFor(FileType.BLOCKS, chunk)
        return openShared(file)
    }

    fun exists(file: String): Boolean {
        return targetStorage.current.exists(file)
    }

    fun openShared(file: String): BlocksFileAccess {
        return currentWriters.get(file) {
            openExclusively(file)
        }
    }

    fun openExclusively(file: String): BlocksFileAccess {
        log.info("Save blocks to $file")
        val dataFileWriter: DataFileWriter<Block>
        val datumWriter = SpecificDatumWriter<Block>(Block::class.java)
        dataFileWriter = DataFileWriter<Block>(datumWriter)
        dataFileWriter.setCodec(CodecFactory.snappyCodec())
        val outputStream = targetStorage.current.createWriter(file)
        val fileAccess = LocalFileAccess(dataFileWriter.create(Block.getClassSchema(), outputStream), runConfig, file, currentWriters, targetStorage.current)
        return if (runConfig.deduplicate) {
            DeduplicatedFileAccess(fileAccess)
        } else {
            fileAccess
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

    class DeduplicatedFileAccess(
        private val delegate: BlocksFileAccess,
    ) : BlocksFileAccess {
        private val written = HashSet<String>()

        override fun append(block: BlockDetails) {
            // don't deduplicate here, it is done in append(Datum) which is called by this method
            delegate.append(block)
        }

        override fun append(block: Block) {
            if (written.contains(block.blockId)) {
                return
            }
            delegate.append(block)
            written.add(block.blockId)
        }

        override fun close() {
            delegate.close()
        }
    }
}
