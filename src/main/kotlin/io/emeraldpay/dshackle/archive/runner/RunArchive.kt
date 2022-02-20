package io.emeraldpay.dshackle.archive.runner

import io.emeraldpay.dshackle.archive.BlocksRange
import io.emeraldpay.dshackle.archive.config.RunConfig
import io.emeraldpay.dshackle.archive.storage.CompleteWriter
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import kotlin.system.exitProcess
import org.springframework.context.annotation.Profile

@Service
@Profile("run-archive")
class RunArchive(
        @Autowired private val blockSource: BlockSource,
        @Autowired private val completeWriter: CompleteWriter,
        @Autowired private val blocksRange: BlocksRange,
        @Autowired private val runConfig: RunConfig,
        @Autowired private val postArchive: PostArchive,
) : Runnable {

    companion object {
        private val log = LoggerFactory.getLogger(RunArchive::class.java)
    }

    private val chunkedArchive = ChunkedArchive(blocksRange, completeWriter)

    override fun run() {
        log.info("Running archive ${blocksRange.startBlock}..${blocksRange.startBlock + blocksRange.length - 1} using ${runConfig.range.chunk} blocks per file")
        log.info("Include data: ")
        log.info("  Standard  : true")
        log.info("  Tracing   : ${runConfig.options.trace}")
        if (runConfig.options.trace) {
            log.warn("              Tracing is very expensive operation. Results may contain larger than 1Gb JSON per transaction")
        }
        log.info("  StateDiff : ${runConfig.options.stateDiff}")
        if (blocksRange.length <= 0) {
            log.warn("Requested ${blocksRange.length} blocks to archive")
        } else if (blocksRange.startBlock < 0) {
            log.warn("Requested to start from ${blocksRange.startBlock}")
        } else {
            if (blocksRange.isUsingRanges) {
                archiveRanges()
            } else {
                archiveIndividual()
            }
        }
        postArchive.close()
        // make sure it exits after the completion even if there are still running threads
        exitProcess(0)
    }

    fun archiveRanges() {
        chunkedArchive.archiveRanges { chunk ->
            blockSource.getData(chunk.startBlock, chunk.length)
        }
    }

    fun archiveIndividual() {
        blockSource.getData(blocksRange.startBlock, blocksRange.length)
                .transform(completeWriter.streamConsumer())
                .blockLast()
    }
}