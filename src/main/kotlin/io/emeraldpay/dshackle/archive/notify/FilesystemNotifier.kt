package io.emeraldpay.dshackle.archive.notify

import com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import kotlin.io.path.exists

/**
 * Writes notifications into a file in JSONL format
 */
class FilesystemNotifier(
    private val file: Path,
    objectMapper: ObjectMapper,
) : Notifier.JsonBased(objectMapper) {

    companion object {
        private val log = LoggerFactory.getLogger(FilesystemNotifier::class.java)
        private val NL = "\n"
    }

    private val writeScheduler = Schedulers.newSingle("file-notify")

    init {
        if (!file.exists()) {
            if (!file.parent.exists()) {
                Files.createDirectories(file.parent)
            }
        }
    }

    override fun onCreated(archiveJson: String): Mono<Void> {
        return Mono.fromCallable {
            // it opens the file each time because it's not expected to have too many notification so keeping it open, with buffers and so on, doesn't make much sense
            Files.newBufferedWriter(file, StandardOpenOption.CREATE, StandardOpenOption.APPEND)
                .use { wrt ->
                    wrt.write(archiveJson)
                    wrt.write(NL)
                }
        }
            .subscribeOn(writeScheduler)
            .then()
    }
}
