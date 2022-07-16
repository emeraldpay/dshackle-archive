package io.emeraldpay.dshackle.archive.runner

import io.emeraldpay.dshackle.archive.BlocksRange
import io.emeraldpay.dshackle.archive.FileType
import io.emeraldpay.dshackle.archive.model.AlignedChunkIterator
import io.emeraldpay.dshackle.archive.model.Chunk
import io.emeraldpay.dshackle.archive.storage.CompleteWriter
import java.util.function.Function
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono


@Service
@Profile("run-fix")
class RunFix(
        @Autowired private val blockSource: BlockSource,
        @Autowired private val completeWriter: CompleteWriter,
        @Autowired private val blocksRange: BlocksRange,
        @Autowired private val scanningTools: ScanningTools,
        @Autowired private val rangeTools: RangeTools,
): RunCommand {

    companion object {
        private val log = LoggerFactory.getLogger(RunFix::class.java)
    }

    //TODO same as in RunArchive
    private val data = { chunk: Chunk ->
        blockSource.getData(chunk.startBlock, chunk.length)
    }

    override fun run(): Mono<Void> {
        return rangeTools.checkStartBlock()
                .flatMap {
                    val range = it.wholeChunk()
                    val missing: Flux<Chunk> = getMissing(range)
                            .switchIfEmpty {
                                Mono.fromCallable { log.info("No broken ranges") }
                                        .then(Mono.empty<Chunk>())
                            }
                            .doOnError { t -> log.error("Failed to find missing blocks", t) }

                    archive(missing)
                            .doOnError { t -> log.error("Failed to fix missing blocks", t) }
                            .then()
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
                .transform(align())
    }

    fun fullyArchived(): Function<Flux<ScanningTools.FileChunk>, Flux<Chunk>> {
        return Function { files ->
            files
                    // get all files for a same range
                    .groupBy { it.chunk }
                    .flatMap { it.take(2).collectList() }
                    // get ranges which has both blocks and transactions
                    .filter { it.any { f -> f.type == FileType.TRANSACTIONS } && it.any { f -> f.type == FileType.BLOCKS } }
                    .map { it.first().chunk }
        }
    }

    fun findBroken(wholeRange: Chunk): Function<Flux<Chunk>, Flux<Chunk>> {
        return Function { chunks ->
            chunks
                    .reduce(ScanningTools.Report.empty(), ScanningTools.Report::withChunk)
                    .map { findDifference(wholeRange, it.chunks) }
                    .doOnNext {
                        val total = it.sumOf(Chunk::length)
                        log.info("Total blocks to fix: $total")
                    }
                    .flatMapMany {
                        Flux.fromIterable(it)
                    }
        }
    }

    fun align(): Function<Flux<Chunk>, Flux<Chunk>> {
        return Function { all ->
            all.flatMap {
                val iter = AlignedChunkIterator(it.startBlock, it.length, blocksRange.range.chunk)
                Flux.fromIterable(iter.getChunks())
            }
        }
    }

    fun findDifference(target: Chunk, chunks: List<Chunk>): List<Chunk> {
        return chunks.fold(listOf(target)) { all, c ->
            val results = mutableListOf<Chunk>()
            all.forEach { expected ->
                if (expected.intersects(c)) {
                    if (expected.includes(c)) {
                        results.addAll(expected.splitBy(c).toList())
                    } else {
                        results.add(expected.cut(c))
                    }
                } else {
                    results.add(expected)
                }
            }
            results.filter { !it.isEmpty }
        }
    }

}