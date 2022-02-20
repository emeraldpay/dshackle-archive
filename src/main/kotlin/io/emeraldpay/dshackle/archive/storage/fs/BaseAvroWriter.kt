package io.emeraldpay.dshackle.archive.storage.fs

import io.emeraldpay.dshackle.archive.storage.CurrentStorage
import java.nio.file.Files
import java.nio.file.Path
import org.apache.avro.file.DataFileWriter
import org.slf4j.LoggerFactory

abstract class BaseAvroWriter<T>(
        val dataFileWriter: DataFileWriter<T>,
        private val path: Path,
        private val currentStorage: CurrentStorage,
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
            Files.deleteIfExists(path)
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