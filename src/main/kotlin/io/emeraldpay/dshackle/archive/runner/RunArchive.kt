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
        @Autowired private val runConfig: RunConfig,
        @Autowired private val rangeTools: RangeTools,
) : RunCommand {

    companion object {
        private val log = LoggerFactory.getLogger(RunArchive::class.java)
    }

    override fun run(): Mono<Void> {
        log.info("Include data: ")
        log.info("  Standard  : true")
        log.info("  Tracing   : ${runConfig.options.trace}")
        if (runConfig.options.trace) {
            log.warn("              Tracing is very expensive operation. Results may contain larger than 1Gb JSON per transaction")
        }
        log.info("  StateDiff : ${runConfig.options.stateDiff}")
        return rangeTools.checkStartBlock()
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