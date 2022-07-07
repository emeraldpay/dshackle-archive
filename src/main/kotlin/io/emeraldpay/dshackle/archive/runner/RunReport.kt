package io.emeraldpay.dshackle.archive.runner

import io.emeraldpay.dshackle.archive.BlocksRange
import io.emeraldpay.dshackle.archive.FileType
import io.emeraldpay.dshackle.archive.config.RunConfig
import io.emeraldpay.dshackle.archive.model.Chunk
import io.emeraldpay.dshackle.archive.storage.FilenameGenerator
import io.emeraldpay.dshackle.archive.storage.RangeAccess
import io.emeraldpay.dshackle.archive.storage.SourceStorage
import java.util.function.Function
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono
import reactor.util.function.Tuple2
import reactor.util.function.Tuples

@Service
@Profile("run-report")
class RunReport(
        @Autowired private val runConfig: RunConfig,
        @Autowired private val blocksRange: BlocksRange,
        @Autowired private val filenameGenerator: FilenameGenerator,
        @Autowired private val sourceStorage: SourceStorage,
): RunCommand {

    companion object {
        private val log = LoggerFactory.getLogger(RunReport::class.java)
    }

    private val rangeAccess = RangeAccess(runConfig)

    override fun run(): Mono<Void> {
        val wholeChunk = blocksRange.wholeChunk()
        return getRange()
                .doOnSubscribe {
                    log.info("Checking archive files. It may take several minutes.")
                }
                .map { rangeAccess.findHeightsToCheck(it.t1, it.t2) }
                .transform(reportForHeights(wholeChunk))
                .transform(printReport(wholeChunk))
                .then()
    }

    fun reportForHeights(wholeChunk: Chunk): Function<Mono<List<Long>>,Mono<SummaryReport>> {
        return Function { src ->
            src.flatMap { heights ->
                val allFiles = sourceStorage.current.listArchive(heights)
                        .flatMap {
                            val type = filenameGenerator.extractType(it)
                                    ?.let(FileType.Companion::fromFilenameType)
                            val range = filenameGenerator.parseRange(it)
                            when {
                                type == null || range == null -> Mono.empty()
                                !wholeChunk.intersects(range) -> Mono.empty()
                                else -> Mono.just(FileChunk(type, range))
                            }
                        }
                        .share()
                val blocks = allFiles
                          .filter { it.type == FileType.BLOCKS }
                          .map { it.chunk }
                          .reduce(Report.empty(), Report::withChunk)
                val txes = allFiles
                        .filter { it.type == FileType.TRANSACTIONS }
                        .map { it.chunk }
                        .reduce(Report.empty(), Report::withChunk)
                Mono.zip(blocks, txes).map {
                    SummaryReport(it.t1, it.t2)
                }
            }
        }
    }

    fun getRange(): Mono<Tuple2<Long, Long>> {
        // TODO should also work if user didn't specify any range, in this case it should check the blockchain
        return Mono.just(Tuples.of(blocksRange.startBlock, blocksRange.endBlock))
    }

    fun printReport(wholeChunk: Chunk): Function<Mono<SummaryReport>, Mono<Void>> {
        val printBlocks = { report: Report ->
            val uniqBlocks = report.chunks.sumOf { it.length }
            log.info("Files count: ${report.files}")
            log.info("Actual blocks: ${report.blocks} (with duplicates) / $uniqBlocks (uniq blocks)")
        }
        return Function<Mono<SummaryReport>, Mono<Void>> { src ->
            src.doOnNext { report ->
                log.info("================== REPORT ==================")
                log.info("Expect blocks: ${wholeChunk.length}")
                log.info("BLOCKS:")
                printBlocks(report.blocks)
                log.info("TRANSACTIONS:")
                printBlocks(report.txes)
                log.info("============================================")
            }.then()
        }
    }

    data class FileChunk(
            val type: FileType,
            val chunk: Chunk,
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
}