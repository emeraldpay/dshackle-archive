package io.emeraldpay.dshackle.archive.runner

import io.emeraldpay.dshackle.archive.BlocksRange
import io.emeraldpay.dshackle.archive.config.RunConfig
import io.emeraldpay.dshackle.archive.model.Chunk
import io.emeraldpay.dshackle.archive.storage.CompleteWriter
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

@Service
@Profile("run-fix")
class RunFix(
    @Autowired private val runConfig: RunConfig,
    @Autowired private val blockSource: BlockSource,
    @Autowired private val completeWriter: CompleteWriter,
    @Autowired private val blocksRange: BlocksRange,
    @Autowired private val scanningTools: ScanningTools,
    @Autowired private val rangeTools: RangeTools,
) : RunCommand, RunFixLogic() {

    companion object {
        private val log = LoggerFactory.getLogger(RunFix::class.java)
    }

    // TODO same as in RunArchive
    private val data = { chunk: Chunk ->
        blockSource.getData(chunk.startBlock, chunk.length)
    }

    private val dryRun = runConfig.dryRun

    override fun run(): Mono<Void> {
        return rangeTools.checkStartBlock()
            .flatMap {
                val range = it.wholeChunk()
                val missing: Flux<Chunk> = getMissing(range)
                    .switchIfEmpty(
                        Mono.fromCallable { log.info("No broken ranges") }
                            .then(Mono.empty()),
                    )
                    .doOnError { t -> log.error("Failed to find missing blocks", t) }

                if (dryRun) {
                    missing
                        .doOnNext { chunk ->
                            log.info("Fix range ${chunk.startBlock}..${chunk.endBlock} (DRY RUN)")
                        }
                        .then()
                } else {
                    archive(missing)
                        .doOnError { t -> log.error("Failed to fix missing blocks", t) }
                        .then()
                }
            }
    }

    fun archive(chunks: Flux<Chunk>): Mono<Void> {
        return ChunkedArchive(data, chunks, completeWriter)
            .archiveRanges()
    }

    fun getMissing(range: Chunk): Flux<Chunk> {
        return scanningTools.scanArchives(range)
            .doOnSubscribe {
                log.info("Scan for missing ranges. May take several minutes...")
            }
            .transform(fullyArchived())
            .transform(findBroken(range))
            .transform(align(blocksRange))
            .doOnError { t ->
                log.error("Failed to gather missing chunks", t)
            }
    }
}
