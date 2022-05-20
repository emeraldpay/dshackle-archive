package io.emeraldpay.dshackle.archive.notify

import com.fasterxml.jackson.databind.ObjectMapper
import io.emeraldpay.dshackle.archive.FileType
import io.emeraldpay.dshackle.archive.config.RunConfig
import java.time.Instant
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono

/**
 * A notifier for event related to archive creation. Ex. called when an archive file is created
 */
interface Notifier {

    companion object {
        private val log = LoggerFactory.getLogger(Notifier::class.java)
    }

    /**
     * Called when a file with archive created
     */
    fun onCreated(archive: ArchiveCreated): Mono<Void>

    /**
     * Basic abstract implementation which encoded event as JSON
     */
    abstract class JsonBased(
            private val objectMapper: ObjectMapper,
    ): Notifier {
        override fun onCreated(archive: ArchiveCreated): Mono<Void> {
            val json = objectMapper.writeValueAsString(archive)
            return onCreated(json)
        }

        abstract fun onCreated(archiveJson: String): Mono<Void>
    }

    data class ArchiveCreated(
            val version: String = "https://schema.emrld.io/dshackle-archive/notify",
            val ts: Instant,
            val blockchain: String,
            val type: FileType,
            val run: RunConfig.Command,

            // start and end height for the file.
            val heightStart: Long,
            val heightEnd: Long,

            // full location including the target storage
            // ex. for NAS: smb://10.0.1.15/dshackle-archive/btc/000700000/range-000723745_000723749.block.avro
            // or for Google Storage:  gs://dshackle-archive-bucket/btc/000700000/range-000723745_000723749.block.avro
            val location: String,
    )
}