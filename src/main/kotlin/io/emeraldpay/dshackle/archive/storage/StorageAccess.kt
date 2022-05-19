package io.emeraldpay.dshackle.archive.storage

import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

interface StorageAccess {

    fun listArchive(height: List<Long>? = null): Flux<String>
    fun deleteArchives(files: List<String>): Mono<Void>

    /**
     * Get a _full_ URL to the file under current storage, to access by an external service.
     */
    fun locationFor(file: String): String
}