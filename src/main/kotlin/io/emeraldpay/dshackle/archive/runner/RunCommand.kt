package io.emeraldpay.dshackle.archive.runner

import reactor.core.publisher.Mono

interface RunCommand {

    fun run(): Mono<Void>

}