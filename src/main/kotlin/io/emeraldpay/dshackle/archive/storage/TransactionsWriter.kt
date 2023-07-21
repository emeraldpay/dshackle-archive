package io.emeraldpay.dshackle.archive.storage

import com.linkedin.avro.fastserde.FastSpecificDatumWriter
import io.emeraldpay.dshackle.archive.FileType
import io.emeraldpay.dshackle.archive.avro.Transaction
import io.emeraldpay.dshackle.archive.config.RunConfig
import io.emeraldpay.dshackle.archive.model.Chunk
import org.apache.avro.AvroRuntimeException
import org.apache.avro.file.CodecFactory
import org.apache.avro.file.DataFileWriter
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Repository
import java.time.Instant
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

@Repository
class TransactionsWriter(
    @Autowired private val configuredFilenameGenerator: ConfiguredFilenameGenerator,
    @Autowired private val runConfig: RunConfig,
    @Autowired private val targetStorage: TargetStorage,
) {

    companion object {
        private val log = LoggerFactory.getLogger(TransactionsWriter::class.java)
    }

    private val currentWriters = CurrentStorage(100)

    fun openShared(chunk: Chunk): TxFileAccess {
        val file = configuredFilenameGenerator.fileFor(FileType.TRANSACTIONS, chunk)
        return openShared(file)
    }

    fun exists(file: String): Boolean {
        return targetStorage.current.exists(file)
    }

    fun openShared(file: String): TxFileAccess {
        return currentWriters.get(file) {
            openExclusively(file)
        }
    }

    fun openExclusively(file: String): TxFileAccess {
        log.info("Save transactions to $file")

        val datumWriter = FastSpecificDatumWriter<Transaction>(Transaction.getClassSchema())
        val dataFileWriter: DataFileWriter<Transaction> = DataFileWriter<Transaction>(datumWriter)
        dataFileWriter.setCodec(CodecFactory.snappyCodec())
        dataFileWriter.setSyncInterval(1 * 1024 * 1024) // flush every megabyte

        val outputStream = targetStorage.current.createWriter(file)

        val fileAccess = LocalFileAccess(dataFileWriter.create(Transaction.getClassSchema(), outputStream), runConfig, file, currentWriters, targetStorage.current)
        return if (runConfig.deduplicate) {
            DeduplicatedFileAccess(fileAccess)
        } else {
            fileAccess
        }
    }

    fun closeAll() {
        currentWriters.close()
    }

    interface TxFileAccess : AutoCloseable {
        fun append(block: BlockDetails, tx: TransactionDetails)
        fun append(datum: Transaction)
    }

    open class LocalFileAccess(
        dataFileWriter: DataFileWriter<Transaction>,
        private val runConfig: RunConfig,
        path: String,
        currentStorage: CurrentStorage,
        access: StorageAccess,
    ) : TxFileAccess, AutoCloseable, BaseAvroWriter<Transaction>(dataFileWriter, path, currentStorage, access) {

        private val writeLock = ReentrantLock()

        override fun append(block: BlockDetails, tx: TransactionDetails) {
            val datum = Transaction().apply {
                blockchainType = runConfig.chainType
                blockchainId = runConfig.getChainId()
                archiveTimestamp = Instant.now()

                blockId = block.hash
                timestamp = block.timestamp
                height = block.height

                // note: it would be -1 if it's not from that block
                index = block.transactionHashes.indexOfFirst { it == tx.hash }.toLong()
                txid = tx.hash
                from = tx.from
                to = tx.to

                json = tx.json
                receiptJson = tx.receiptJson
                raw = tx.raw
                traceJson = tx.traceJson
                stateDiffJson = tx.stateDiff
            }
            this.append(datum)
        }

        override fun append(datum: Transaction) {
            try {
                writeLock.withLock {
                    dataFileWriter.append(datum)
                }
            } catch (t: AvroRuntimeException) {
                log.error("Failed to write tx: ${datum.height} ${datum.txid}. Error ${t.javaClass} ${t.message}")
                drop()
                throw t
            }
        }
    }

    class DeduplicatedFileAccess(
        private val delegate: TxFileAccess,
    ) : TxFileAccess {

        private val written = HashSet<String>()

        override fun append(block: BlockDetails, tx: TransactionDetails) {
            // don't deduplicate here, it is done in append(Datum) which is called by this method
            delegate.append(block, tx)
        }

        override fun append(datum: Transaction) {
            if (written.contains(datum.txid)) {
                return
            }
            delegate.append(datum)
            written.add(datum.txid)
        }

        override fun close() {
            delegate.close()
        }
    }
}
