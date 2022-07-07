package io.emeraldpay.dshackle.archive.runner

import io.emeraldpay.dshackle.archive.BlocksRange
import io.emeraldpay.dshackle.archive.config.RunConfig
import io.emeraldpay.dshackle.archive.model.Chunk
import io.emeraldpay.dshackle.archive.storage.CompleteWriter
import io.emeraldpay.dshackle.archive.storage.FilenameGenerator
import io.emeraldpay.dshackle.archive.storage.RangeAccess
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

    private val rangeAccess = RangeAccess(runConfig)

    override fun run(): Mono<Void> {
        log.info("Include data: ")
        log.info("  Standard  : true")
        log.info("  Tracing   : ${runConfig.options.trace}")
        if (runConfig.options.trace) {
            log.warn("              Tracing is very expensive operation. Results may contain larger than 1Gb JSON per transaction")
        }
        log.info("  StateDiff : ${runConfig.options.stateDiff}")
        return checkStartBlock()
                .doOnNext { blocksRange ->
                    log.info("Running archive ${blocksRange.startBlock}..${blocksRange.endBlock} using ${runConfig.range.chunk} blocks per file")
                }
                .flatMap(::runPrepared)
    }

    fun runPrepared(blocksRange: BlocksRange): Mono<Void> {
        return if (blocksRange.length <= 0) {
            log.warn("Requested ${blocksRange.length} blocks to archive")
            Mono.empty()
        } else if (blocksRange.startBlock < 0) {
            log.warn("Requested to start from ${blocksRange.startBlock}")
            Mono.empty()
        } else {
            if (blocksRange.isUsingRanges) {
                archiveRanges(ChunkedArchive(blocksRange, completeWriter))
            } else {
                archiveIndividual(blocksRange)
            }
        }
    }

    fun checkStartBlock(): Mono<BlocksRange> {
        return if (runConfig.range.continueFromLast) {
            log.debug("Check for last archive in range")
            val heights = rangeAccess.findHeightsToCheck(blocksRange)
            val wholeChunk = blocksRange.wholeChunk()
            targetStorage.current.listArchive(heights)
                    .flatMap { Mono.justOrEmpty(filenameGenerator.parseRange(it)).cast(Chunk::class.java) }
                    .filter(wholeChunk::intersects)
                    .switchIfEmpty(Mono.fromCallable { log.debug("First run in the selected range") }.then(Mono.empty()))
                    .map(Chunk::endBlock)
                    .reduce(Long::coerceAtLeast)
                    .map { currentBlock ->
                        log.debug("Continue from $currentBlock")
                        blocksRange.length = blocksRange.startBlock + blocksRange.length - currentBlock
                        blocksRange.startBlock = currentBlock
                        blocksRange
                    }
        } else {
            Mono.just(blocksRange)
        }
    }

    fun archiveRanges(chunkedArchive: ChunkedArchive): Mono<Void> {
        return chunkedArchive.archiveRanges { chunk ->
            blockSource.getData(chunk.startBlock, chunk.length)
        }
    }

    fun archiveIndividual(blocksRange: BlocksRange): Mono<Void> {
        return blockSource.getData(blocksRange.startBlock, blocksRange.length)
                .transform(completeWriter.streamConsumer())
                .then()
    }
}