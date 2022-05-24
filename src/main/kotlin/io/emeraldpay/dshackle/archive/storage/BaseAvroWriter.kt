package io.emeraldpay.dshackle.archive.storage

import java.nio.file.Files
import java.nio.file.Path
import org.apache.avro.file.DataFileWriter
import org.slf4j.LoggerFactory

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
        dataFileWriter.close()
        currentStorage.remove(path)
    }
}