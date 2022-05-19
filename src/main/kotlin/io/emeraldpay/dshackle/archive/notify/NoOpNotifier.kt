package io.emeraldpay.dshackle.archive.notify

import reactor.core.publisher.Mono

/**
 * Doesn't make any notifications
 */
class NoOpNotifier: Notifier {

    override fun onCreated(archive: Notifier.ArchiveCreated): Mono<Void> {
        return Mono.fromRunnable {  }
    }

}