package io.emeraldpay.dshackle.archive.notify

import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

class MultiNotifier(
    private val delegates: List<Notifier>,
) : Notifier {

    companion object {
        private val log = LoggerFactory.getLogger(MultiNotifier::class.java)
    }

    override fun onCreated(archive: Notifier.ArchiveCreated): Mono<Void> {
        return Flux.fromIterable(delegates)
            .flatMap { notifier ->
                notifier.onCreated(archive)
                    .map { true }
                    // note that it ignores the failed notifications
                    .doOnError { t -> log.warn("Failed to submit through Notifier ${notifier.javaClass}. ${t.javaClass}: ${t.message}") }
                    .onErrorReturn(false)
            }
            .then()
    }
}
