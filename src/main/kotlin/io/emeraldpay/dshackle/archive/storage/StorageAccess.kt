package io.emeraldpay.dshackle.archive.storage

import java.io.InputStream
import java.io.OutputStream
import org.apache.avro.file.SeekableInput
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

interface StorageAccess {

    fun listArchive(height: Long): Flux<String>
    fun deleteArchives(files: List<String>): Mono<Void>

    /**
     * Get a _full_ URI to the file under current storage, to access by an external service.
     */
    fun getURI(file: String): String

    /**
     * Creates a new writer to put data to the storage at path
     */
    fun createWriter(path: String): OutputStream

    /**
     * Read an existing file in the archive
     */
    fun createReader(path: String): SeekableInput
}