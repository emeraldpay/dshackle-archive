package io.emeraldpay.dshackle.archive.notify

import com.fasterxml.jackson.databind.ObjectMapper
import io.emeraldpay.dshackle.archive.FileType
import io.emeraldpay.dshackle.archive.config.RunConfig
import io.emeraldpay.dshackle.archive.model.Chunk
import java.nio.file.Path
import java.time.Instant
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.Lifecycle
import org.springframework.context.SmartLifecycle
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono

@Service
class CurrentNotifier(
        @Autowired private val runConfig: RunConfig,
        @Autowired private val objectMapper: ObjectMapper,
): Notifier, Lifecycle, SmartLifecycle {

    companion object {
        private val log = LoggerFactory.getLogger(CurrentNotifier::class.java)
    }

    private val delegate: Notifier
    private val lifecycle: List<Lifecycle>

    init {
        val config = runConfig.notify
        val notifiers = mutableListOf<Notifier>()
        val lifecycle = mutableListOf<Lifecycle>()

        if (config.file != null) {
             notifiers.add(
                     FilesystemNotifier(Path.of(config.file), objectMapper)
             )
        }
        if (config.directory != null) {
            val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMddhhmmss")
                    .withZone(ZoneId.systemDefault())
            val filename = listOf(
                    "dshackle-archive",
                    formatter.format(Instant.now()),
                    "jsonl"
            ).joinToString(".")
            notifiers.add(
                    FilesystemNotifier(Path.of(config.directory, filename), objectMapper)
            )
        }
        if (config.pubsub != null) {
            val notifier = PubsubNotifier(config.pubsub, objectMapper)
            notifiers.add(notifier)
            lifecycle.add(notifier)
        }

        delegate = if (notifiers.size == 1) {
            notifiers.first()
        } else if (notifiers.size > 1) {
            MultiNotifier(notifiers)
        } else {
            NoOpNotifier()
        }
        this.lifecycle = lifecycle
    }

    override fun onCreated(archive: Notifier.ArchiveCreated): Mono<Void> {
        return delegate.onCreated(archive)
    }

    fun createEvent(type: FileType, chunk: Chunk, location: String): Notifier.ArchiveCreated {
        return createEvent(type, chunk.startBlock, chunk.endBlock, location)
    }

    fun createEvent(type: FileType, heightStart: Long, heightEnd: Long, location: String): Notifier.ArchiveCreated {
        return Notifier.ArchiveCreated(
                ts = Instant.now(),
                blockchain = runConfig.getChainId(),
                run = runConfig.command,
                type = type,
                heightStart = heightStart,
                heightEnd = heightEnd,
                location = location
        )
    }

    override fun start() {
        lifecycle.forEach(Lifecycle::start)
    }

    override fun stop() {
        lifecycle.forEach(Lifecycle::stop)
    }

    override fun isRunning(): Boolean {
        return lifecycle.any(Lifecycle::isRunning)
    }
}