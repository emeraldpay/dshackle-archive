package io.emeraldpay.dshackle.archive.runner

import io.emeraldpay.dshackle.archive.BlocksRange
import io.emeraldpay.dshackle.archive.config.RunConfig
import io.emeraldpay.dshackle.archive.storage.CompleteWriter
import io.emeraldpay.dshackle.archive.storage.FilenameGenerator
import io.emeraldpay.dshackle.archive.storage.TargetStorage
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import org.springframework.context.annotation.Profile
import reactor.core.publisher.Mono

@Service
@Profile("run-archive")
class RunArchive(
        @Autowired private val blockSource: BlockSource,
        @Autowired private val completeWriter: CompleteWriter,
        @Autowired private val blocksRange: BlocksRange,
        @Autowired private val runConfig: RunConfig,
        @Autowired private val targetStorage: TargetStorage,
        @Autowired private val filenameGenerator: FilenameGenerator
) : RunCommand {

    companion object {
        private val log = LoggerFactory.getLogger(RunArchive::class.java)
    }

    private val chunkedArchive = ChunkedArchive(blocksRange, completeWriter)

    override fun run(): Mono<Void> {
        return checkStartBlock()
                .then(runPrepared())
    }

    fun runPrepared(): Mono<Void> {
        log.info("Running archive ${blocksRange.startBlock}..${blocksRange.startBlock + blocksRange.length - 1} using ${runConfig.range.chunk} blocks per file")
        log.info("Include data: ")
        log.info("  Standard  : true")
        log.info("  Tracing   : ${runConfig.options.trace}")
        if (runConfig.options.trace) {
            log.warn("              Tracing is very expensive operation. Results may contain larger than 1Gb JSON per transaction")
        }
        log.info("  StateDiff : ${runConfig.options.stateDiff}")
        return if (blocksRange.length <= 0) {
            log.warn("Requested ${blocksRange.length} blocks to archive")
            Mono.empty()
        } else if (blocksRange.startBlock < 0) {
            log.warn("Requested to start from ${blocksRange.startBlock}")
            Mono.empty()
        } else {
            if (blocksRange.isUsingRanges) {
                archiveRanges()
            } else {
                archiveIndividual()
            }
        }
    }

    fun checkStartBlock(): Mono<Void> {
        return if (runConfig.range.continueFromLast) {
            log.debug("Check for last archive in range")
            val heights = findHeightsToCheck()
            val wholeChunk = blocksRange.wholeChunk()
            targetStorage.current.listArchive(heights)
                    .flatMap { Mono.justOrEmpty(filenameGenerator.parseRange(it)).cast(BlocksRange.Chunk::class.java) }
                    .filter(wholeChunk::intersects)
                    .switchIfEmpty(Mono.fromCallable { log.debug("First run in the selected range") }.then(Mono.empty()))
                    .map(BlocksRange.Chunk::getEndBlock)
                    .reduce(Long::coerceAtLeast)
                    .doOnNext { currentBlock ->
                        log.debug("Continue from $currentBlock")
                        blocksRange.startBlock = currentBlock
                    }
                    .then()
        } else {
            Mono.empty()
        }
    }

    fun findHeightsToCheck(): List<Long> {
        val heights = mutableSetOf<Long>()
        heights.add(blocksRange.startBlock)
        var height = blocksRange.startBlock
        while (height < blocksRange.endBlock) {
            height += runConfig.range.chunk / 2
            heights.add(height)
        }
        return heights.toList()
    }

    fun archiveRanges(): Mono<Void> {
        return chunkedArchive.archiveRanges { chunk ->
            blockSource.getData(chunk.startBlock, chunk.length)
        }
    }

    fun archiveIndividual(): Mono<Void> {
        return blockSource.getData(blocksRange.startBlock, blocksRange.length)
                .transform(completeWriter.streamConsumer())
                .then()
    }
}