package io.emeraldpay.dshackle.archive.storage

import org.apache.avro.file.DataFileWriter
import org.slf4j.LoggerFactory
import java.nio.channels.ClosedChannelException

abstract class BaseAvroWriter<T>(
    val dataFileWriter: DataFileWriter<T>,
    private val path: String,
    private val currentStorage: CurrentStorage,
    private val access: StorageAccess,
) : AutoCloseable {

    companion object {
        private val log = LoggerFactory.getLogger(BaseAvroWriter::class.java)
    }

    fun drop() {
        log.debug("Drop file $path")
        try {
            dataFileWriter.close()
        } catch (t: Throwable) {
            log.warn("Failed to close $path. ${t.javaClass}: ${t.message}")
        }
        try {
            access.deleteArchives(listOf(path)).block()
        } catch (t: Throwable) {
            log.warn("Failed to delete $path. ${t.javaClass}: ${t.message}")
        }
        currentStorage.remove(path)
    }

    override fun close() {
        try {
            log.debug("Closing file: {}", path)
            if (log.isDebugEnabled) {
                val blockCount = DataFileWriter::class.java.getDeclaredField("blockCount")
                blockCount.isAccessible = true
                blockCount.get(dataFileWriter)?.let {
                    log.debug("DataFileWriter {} blockCount before flush: {}", path, it)
                }
            }
            dataFileWriter.flush()
            if (log.isDebugEnabled) {
                val blockCount = DataFileWriter::class.java.getDeclaredField("blockCount")
                blockCount.isAccessible = true
                blockCount.get(dataFileWriter)?.let {
                    log.debug("DataFileWriter {} blockCount after flush: {}", path, it)
                }
            }
            dataFileWriter.close()
            currentStorage.remove(path)
        } catch (e: ClosedChannelException) {
            throw IllegalStateException("Failed to close $path", e)
        }
    }
}
