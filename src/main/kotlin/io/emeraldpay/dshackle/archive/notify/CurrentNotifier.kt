package io.emeraldpay.dshackle.archive.notify

import com.fasterxml.jackson.databind.ObjectMapper
import io.emeraldpay.dshackle.archive.BlocksRange
import io.emeraldpay.dshackle.archive.FileType
import io.emeraldpay.dshackle.archive.config.RunConfig
import java.nio.file.Path
import java.time.Instant
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono

@Service
class CurrentNotifier(
        @Autowired private val runConfig: RunConfig,
        @Autowired private val objectMapper: ObjectMapper,
): Notifier {

    companion object {
        private val log = LoggerFactory.getLogger(CurrentNotifier::class.java)
    }

    private val delegate: Notifier

    init {
        if (runConfig.notify == null) {
            delegate = NoOpNotifier()
        } else {
            val config = runConfig.notify
            if (config.file != null) {
                delegate = FilesystemNotifier(
                        Path.of(config.file), objectMapper
                )
            } else if (config.directory != null) {
                val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMddhhmmss")
                        .withZone(ZoneId.systemDefault())
                val filename = listOf(
                        "dshackle-archive",
                        formatter.format(Instant.now()),
                        "jsonl"
                ).joinToString(".")
                delegate = FilesystemNotifier(
                        Path.of(config.directory, filename), objectMapper
                )
            } else {
                delegate = NoOpNotifier()
            }
        }
    }

    override fun onCreated(archive: Notifier.ArchiveCreated): Mono<Void> {
        return delegate.onCreated(archive)
    }

    fun createEvent(type: FileType, chunk: BlocksRange.Chunk, location: String): Notifier.ArchiveCreated {
        return createEvent(type, chunk.startBlock, chunk.getEndBlock(), location)
    }

    fun createEvent(type: FileType, heightStart: Long, heightEnd: Long, location: String): Notifier.ArchiveCreated {
        return Notifier.ArchiveCreated(
                ts = Instant.now(),
                blockchain = runConfig.getChainId(),
                type = type,
                heightStart = heightStart,
                heightEnd = heightEnd,
                location = location
        )
    }
}