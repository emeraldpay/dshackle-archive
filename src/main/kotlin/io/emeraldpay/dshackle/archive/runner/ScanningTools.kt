package io.emeraldpay.dshackle.archive.runner

import io.emeraldpay.dshackle.archive.FileType
import io.emeraldpay.dshackle.archive.model.Chunk
import io.emeraldpay.dshackle.archive.storage.FilenameGenerator
import io.emeraldpay.dshackle.archive.storage.SourceStorage
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux

@Service
@Profile("run-report", "run-fix", "run-verify")
class ScanningTools(
        @Autowired private val filenameGenerator: FilenameGenerator,
        @Autowired private val sourceStorage: SourceStorage,
) {

    companion object {
        private val log = LoggerFactory.getLogger(ScanningTools::class.java)
    }

    data class FileChunk(
            /**
             * Type of the file
             */
            val type: FileType,
            /**
             * Archive range contained in the file
             */
            val chunk: Chunk,
            /**
             * Relative path to the file inside the storage
             */
            val path: String,
    )

    data class Report(
            val files: Int,
            val blocks: Long,
            val chunks: List<Chunk>,
    ) {

        companion object {
            fun empty(): Report {
                return Report(0, 0, emptyList())
            }
        }

        fun withChunk(chunk: Chunk): Report {
            return Report(
                    this.files + 1,
                    this.blocks + chunk.length,
                    chunk.mergeContinuing(this.chunks)
            )
        }
    }

    data class SummaryReport(
            val blocks: Report,
            val txes: Report,
    )

    fun getSummary(wholeChunk: Chunk): Mono<SummaryReport> {
        val allFiles = scanArchives(wholeChunk)
                .share()
                .cache()
        val blocks = allFiles
                .filter { it.type == FileType.BLOCKS }
                .map { it.chunk }
                .reduce(Report.empty(), Report::withChunk)
        val txes = allFiles
                .filter { it.type == FileType.TRANSACTIONS }
                .map { it.chunk }
                .reduce(Report.empty(), Report::withChunk)
        return Mono.zip(blocks, txes).map {
            SummaryReport(it.t1, it.t2)
        }
    }

    fun scanArchives(wholeChunk: Chunk): Flux<FileChunk> {
        return sourceStorage.current.listArchive(wholeChunk.startBlock)
                .takeWhile {
                    val range = filenameGenerator.parseRange(it)
                    range == null || wholeChunk.intersects(range)
                }
                .flatMap { file ->
                    val type = filenameGenerator.extractType(file)
                            ?.let(FileType.Companion::fromFilenameType)
                    val range = filenameGenerator.parseRange(file)
                    when {
                        type == null || range == null -> Mono.empty()
                        !wholeChunk.intersects(range) -> Mono.empty()
                        else -> Mono.just(FileChunk(type, range, file))
                    }
                }
    }
}