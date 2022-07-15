package io.emeraldpay.dshackle.archive.runner

import io.emeraldpay.dshackle.archive.BlocksRange
import io.emeraldpay.dshackle.archive.config.RunConfig
import io.emeraldpay.dshackle.archive.model.Chunk
import io.emeraldpay.dshackle.archive.storage.CompleteWriter
import io.emeraldpay.dshackle.archive.storage.FilenameGenerator
import io.emeraldpay.dshackle.archive.storage.RangeAccess
import io.emeraldpay.dshackle.archive.storage.TargetStorage
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import org.apache.commons.lang3.time.StopWatch
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import org.springframework.context.annotation.Profile
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.util.function.Tuples

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

    private val data = { chunk: Chunk ->
        blockSource.getData(chunk.startBlock, chunk.length)
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
                archiveRanges(blocksRange)
            } else {
                archiveIndividual(blocksRange)
            }
        }
    }

    fun checkStartBlock(): Mono<BlocksRange> {
        return if (runConfig.range.continueFromLast) {
            log.debug("Check for a last existing archive in the range")
            val knownHeight = AtomicLong(0)
            val heights = rangeAccess.findHeightsToCheck(blocksRange)
            val wholeChunk = blocksRange.wholeChunk()
            val timer = StopWatch.createStarted()
            Flux.fromIterable(heights)
                    .doOnNext {
                        if (timer.time > TimeUnit.SECONDS.toMillis(30)) {
                            log.info("Checking height $it")
                            timer.reset()
                            timer.start()
                        }
                    }
                    // Use N-size range of heights while checking, because it may be up to N-times faster, see logic below
                    .buffer(5)
                    .flatMap({ range ->
                        // Get max height for the current chunks, to avoid checking outside the range. otherwise it checks same height multiple times
                        val limit = (range.maxOrNull() ?: 0) + blocksRange.range.chunk
                        // Check chunks in a reverse order so if a higher chunk has a value larger than a lower chunk is not getting checked
                        Flux.fromIterable(range.sorted().asReversed())
                                // Here is skips the lower chunks from unnecessary check
                                .filter { it >= knownHeight.get() }
                                .flatMap(targetStorage.current::listArchive, 1)
                                .flatMap { Mono.justOrEmpty(filenameGenerator.parseRange(it)).cast(Chunk::class.java) }
                                .filter(wholeChunk::intersects)
                                // Here it remembers the height to avoid an unnecessary check by a lower chunk
                                .doOnNext { chunk -> knownHeight.updateAndGet { known -> known.coerceAtLeast(chunk.endBlock) } }
                                // Stop as soon as we reached the upped bound of the range. Otherwise, it can scan the whole storage until the last element,
                                // repeating that for each source height
                                .takeUntil { it.endBlock <= limit }
                                .map(Chunk::endBlock)
                                .map { Tuples.of(true, it) }
                                // If nothing found in the range of heights we mark it empty to start from the last height
                                .switchIfEmpty(Mono.just(Tuples.of(false, 0L)))
                    }, 1)
                    // Stop as soon as we found an empty range
                    .takeWhile { it.t1 }
                    .map { it.t2 }
                    .reduce(Long::coerceAtLeast)
                    .map { currentBlock ->
                        log.debug("Continue from $currentBlock")
                        blocksRange.length = blocksRange.startBlock + blocksRange.length - currentBlock
                        blocksRange.startBlock = currentBlock
                        blocksRange
                    }
                    .defaultIfEmpty(blocksRange)
        } else {
            Mono.just(blocksRange)
        }
    }

    fun archiveRanges(blocksRange: BlocksRange): Mono<Void> {
        return ChunkedArchive(data, Flux.fromIterable(blocksRange.getChunks()), completeWriter)
                .archiveRanges()
    }

    fun archiveIndividual(blocksRange: BlocksRange): Mono<Void> {
        return blockSource.getData(blocksRange.startBlock, blocksRange.length)
                .transform(completeWriter.streamConsumer())
                .then()
    }
}