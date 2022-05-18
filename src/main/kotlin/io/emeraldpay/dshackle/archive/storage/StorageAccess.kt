package io.emeraldpay.dshackle.archive.storage

import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

interface StorageAccess {

    fun listArchive(height: List<Long>? = null): Flux<String>
    fun deleteArchives(files: List<String>): Mono<Void>

}