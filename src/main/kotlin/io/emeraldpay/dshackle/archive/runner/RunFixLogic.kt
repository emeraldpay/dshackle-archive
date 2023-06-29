package io.emeraldpay.dshackle.archive.runner

import io.emeraldpay.dshackle.archive.BlocksRange
import io.emeraldpay.dshackle.archive.FileType
import io.emeraldpay.dshackle.archive.model.AlignedChunkIterator
import io.emeraldpay.dshackle.archive.model.Chunk
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import java.util.function.Function
import reactor.core.publisher.Mono

open class RunFixLogic {

    companion object {
        private val log = LoggerFactory.getLogger(RunFixLogic::class.java)
    }

    fun fullyArchived(): Function<Flux<ScanningTools.FileChunk>, Flux<Chunk>> {
        val counter = ChunkCounter()
        return Function { files ->
            files.flatMap(counter::offer, 1)
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

    fun align(blocksRange: BlocksRange): Function<Flux<Chunk>, Flux<Chunk>> {
        return Function { all ->
            all.flatMap {
                val iter = AlignedChunkIterator(it.startBlock, it.length, blocksRange.range.chunk)
                Flux.fromIterable(iter.getChunks())
            }
        }
    }

    /**
     * A range chunk may contain multiple files, and we consider it as a good chunk when both Blocks and Transactions are exist.
     * The checker keeps all the coming files until a whole chunk is completed, then it return it.
     *
     * Note that it supposed to be used from a single thread.
     */
    class ChunkCounter {
        private val waiting = HashMap<Chunk, MutableSet<FileType>>()

        fun offer(next: ScanningTools.FileChunk): Mono<Chunk> {
            var existing = waiting[next.chunk]
            if (existing == null) {
                existing = mutableSetOf()
                waiting[next.chunk] = existing
            }
            existing.add(next.type)
            val whole = existing.contains(FileType.TRANSACTIONS) && existing.contains(FileType.BLOCKS)
            return if (whole) {
                waiting.remove(next.chunk)
                Mono.just(next.chunk)
            } else {
                Mono.empty()
            }
        }
    }
}
