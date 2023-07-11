package io.emeraldpay.dshackle.archive.runner

import io.emeraldpay.dshackle.archive.BlocksRange
import io.emeraldpay.dshackle.archive.model.Chunk
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono
import java.util.function.Function

@Service
@Profile("run-report")
class RunReport(
    @Autowired private val blocksRange: BlocksRange,
    @Autowired private val scanningTools: ScanningTools,
) : RunCommand {

    companion object {
        private val log = LoggerFactory.getLogger(RunReport::class.java)
    }

    override fun run(): Mono<Void> {
        return getRange()
            .doOnSubscribe { log.info("Checking archive files. It may take several minutes.") }
            .flatMap { range ->
                scanningTools.getSummary(range)
                    .transform(printReport(range))
            }
            .then()
    }

    fun getRange(): Mono<Chunk> {
        // TODO should also work if the user didn't specify any range, in this case it should check the whole blockchain
        return Mono.just(blocksRange.wholeChunk())
    }

    fun printReport(range: Chunk): Function<Mono<ScanningTools.SummaryReport>, Mono<Void>> {
        val printBlocks = { report: ScanningTools.Report ->
            val uniqBlocks = report.chunks.sumOf { it.length }
            log.info("Files count: ${report.files}")
            log.info("Actual blocks: ${report.blocks} (with duplicates) / $uniqBlocks (uniq blocks)")
        }
        return Function<Mono<ScanningTools.SummaryReport>, Mono<Void>> { src ->
            src.doOnNext { report ->
                log.info("================== REPORT ==================")
                log.info("Expect blocks: ${range.length}")
                log.info("BLOCKS:")
                printBlocks(report.blocks)
                log.info("TRANSACTIONS:")
                printBlocks(report.txes)
                log.info("============================================")
            }
                .then()
        }
    }
}
