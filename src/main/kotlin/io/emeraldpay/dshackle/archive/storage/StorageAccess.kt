package io.emeraldpay.dshackle.archive.storage

import reactor.core.publisher.Flux

interface StorageAccess {

    fun listArchive(height: List<Long>? = null): Flux<String>

}