package io.emeraldpay.dshackle.archive.runner

import io.emeraldpay.dshackle.archive.BlocksRange
import io.emeraldpay.dshackle.archive.config.RunConfig
import io.emeraldpay.dshackle.archive.model.Chunk
import io.emeraldpay.dshackle.archive.storage.FilenameGenerator
import io.emeraldpay.dshackle.archive.storage.RangeAccess
import io.emeraldpay.dshackle.archive.storage.TargetStorage
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import org.apache.commons.lang3.time.StopWatch
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.util.function.Tuples

@Profile("run-archive", "run-fix")
@Service
class RangeTools(
        @Autowired private val blocksRange: BlocksRange,
        @Autowired private val runConfig: RunConfig,
        @Autowired(required = false) private val targetStorage: TargetStorage?,
        @Autowired(required = false) private val filenameGenerator: FilenameGenerator?,
        @Autowired(required = false) private val blockSource: BlockSource?,
) {

    companion object {
        private val log = LoggerFactory.getLogger(RangeTools::class.java)
    }

    private val rangeAccess = RangeAccess(runConfig)

    fun checkStartBlock(): Mono<BlocksRange> {
        return if (runConfig.range.continueFromLast) {
            continueFromLast()
        } else if (runConfig.range.backward) {
            goingBack()
        } else {
            Mono.just(blocksRange)
        }
    }

    fun goingBack(): Mono<BlocksRange> {
        log.debug("Calculate a block height to start as backward from the current height")
        check(blockSource != null) {
            "Cannot use the current height when blockchain connection is not set"
        }
        return blockSource.getCurrentHeight().map { height ->
            val startBlock = blocksRange.endBlock.let { delta ->
                height - delta - 1
            }
            val length = blocksRange.length.let { maxLength ->
                maxLength.coerceAtMost(startBlock + maxLength)
            }

            blocksRange.length = length.coerceAtLeast(0)
            blocksRange.startBlock = startBlock.coerceAtLeast(0)
            log.info("Process range ${blocksRange.startBlock}..${blocksRange.endBlock}")
            blocksRange
        }
    }

    fun continueFromLast(): Mono<BlocksRange> {
        log.debug("Check for a last existing archive in the range")
        check(targetStorage != null) {
            "Cannot continue from the last archive when the storage is not available"
        }
        check(filenameGenerator != null) {
            "Cannot continue from the last archive when the storage is not configured"
        }
        val knownHeight = AtomicLong(0)
        val heights = rangeAccess.findHeightsToCheck(blocksRange)
        val wholeChunk = blocksRange.wholeChunk()
        val timer = StopWatch.createStarted()
        return Flux.fromIterable(heights)
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
    }
}