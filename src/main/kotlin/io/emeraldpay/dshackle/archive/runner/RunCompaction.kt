package io.emeraldpay.dshackle.archive.runner

import io.emeraldpay.dshackle.archive.BlocksRange
import io.emeraldpay.dshackle.archive.avro.Block
import io.emeraldpay.dshackle.archive.avro.Transaction
import io.emeraldpay.dshackle.archive.config.RunConfig
import io.emeraldpay.dshackle.archive.model.Chunk
import io.emeraldpay.dshackle.archive.storage.CompleteWriter
import io.emeraldpay.dshackle.archive.storage.FilenameGenerator
import io.emeraldpay.dshackle.archive.storage.SourceStorage
import io.emeraldpay.dshackle.archive.storage.BlocksReader
import io.emeraldpay.dshackle.archive.storage.TransactionsReader
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import java.nio.file.Path
import java.util.concurrent.locks.ReentrantLock
import java.util.function.Function
import kotlin.concurrent.withLock
import kotlin.io.path.name
import kotlin.system.exitProcess
import org.reactivestreams.Publisher
import org.slf4j.LoggerFactory
import reactor.core.publisher.GroupedFlux
import reactor.core.publisher.Mono
import reactor.util.function.Tuples

@Service
@Profile("run-compact")
class RunCompaction(
        @Autowired private val completeWriter: CompleteWriter,
        @Autowired private val runConfig: RunConfig,
        @Autowired private val blocksRange: BlocksRange,
        @Autowired private val filenameGenerator: FilenameGenerator,
        @Autowired private val sourceStorage: SourceStorage,
        @Autowired private val transactionsReader: TransactionsReader,
        @Autowired private val blocksReader: BlocksReader,
        @Autowired(required = false) private val blockSource: BlockSource?,
) : RunCopy(completeWriter, runConfig, blocksRange, sourceStorage, transactionsReader, blocksReader) {

    companion object {
        private val log = LoggerFactory.getLogger(RunCompaction::class.java)
    }

    private val filterBlocks: Function<Flux<Block>, Flux<Block>>
    private val filterTxes: Function<Flux<Transaction>, Flux<Transaction>>

    init {
        if (runConfig.compaction.acceptForks) {
            filterBlocks = Function { it }
            filterTxes = Function { it }
        } else {
            if (blockSource == null) {
                log.warn("Compaction is configured to pack only non-forked blocks which requires active connection to the original blockchain. " +
                        "It's not provided for the current run and Dshackle Archive cannot continue without it. " +
                        "To disable such a verification and accept all data including forks, use option --compact.forks")
                exitProcess(1)
            }
            val filters = ForkFilter(blockSource)
            filterBlocks = filters.filterBlocks()
            filterTxes = filters.filterTxes()
        }
    }

    override fun processBlocks(files: Flux<Path>): Mono<Void> {
        val groups = groupByChunk(files)
        return groups.flatMap {
            openAndConsumeBlocks(it)
        }.then()
    }

    override fun processTransactions(files: Flux<Path>): Mono<Void> {
        val groups = groupByChunk(files)

        return groups.flatMap {
            openAndConsumeTransactions(it)
        }.then()
    }

    /**
     * Group source files into flux of files per chunk
     */
    fun groupByChunk(files: Flux<Path>): Flux<GroupedFlux<Chunk, Path>> {
        val chunks = blocksRange.getChunks()
        val wholeChunk = blocksRange.wholeChunk()
        return files.filter {
                    val isSingle = filenameGenerator.isSingle(it.fileName.name)
                    if (isSingle) {
                        val currentChunk = filenameGenerator.parseRange(it.fileName.name)
                        currentChunk != null && wholeChunk.intersects(currentChunk)
                    } else {
                        false
                    }
                }
                .groupBy {
                    val currentChunk = filenameGenerator.parseRange(it.fileName.name)!!
                    chunks.find { it.intersects(currentChunk) }!!
                }
    }

    fun openAndConsumeTransactions(chunkInputs: Flux<Path>): Mono<Void> {
        return processChunk(chunkInputs, transactionsReader::open) {
            completeWriter.consumeTransactions(
                    it.transform(filterTxes)
            ).then()
        }
    }

    fun openAndConsumeBlocks(chunkInputs: Flux<Path>): Mono<Void> {
        return processChunk(chunkInputs, blocksReader::open) {
            completeWriter.consumeBlocks(
                    it.transform(filterBlocks)
            ).then()
        }
    }

    fun <T> processChunk(chunkInputs: Flux<Path>, open: (Path) -> Publisher<T>, accept: (Flux<T>) -> Mono<Void>): Mono<Void> {
        // here we track all processed files so we can delete them later
        val consumed = mutableListOf<Path>()
        val source = chunkInputs
                // remember the file
                .doOnNext(consumed::add)
                .flatMapSequential(open)

        return accept(source)
                .then(
                        // use Callable to process the consumed files only at the last step, otherwise it may be incomplete
                        Mono.fromCallable { consumed.map { it.toFile().path } }
                                .flatMap(sourceStorage.current::deleteArchives)
                )
                .then()
    }

    class ForkFilter(
            private val blockSource: BlockSource
    ) {

        private val verifyLock = ReentrantLock()
        private val verified = mutableMapOf<Long, Mono<String>>()

        fun isOnMain(blockId: String, height: Long): Mono<Boolean> {
            val hash = verifyLock.withLock {
                val current = verified[height]
                if (current != null) {
                    current
                } else {
                    val requested = blockSource.getBlockIdAtHeight(height).share().doOnCancel {
                        verifyLock.withLock {
                            verified.remove(height)
                        }
                    }
                    verified[height] = requested
                    requested
                }
            }
            return hash.map {
                it == blockId
            }
        }

        fun isOnMain(block: Block): Mono<Boolean> {
            return isOnMain(block.blockId, block.height)
        }

        fun isOnMain(tx: Transaction): Mono<Boolean> {
            return isOnMain(tx.blockId, tx.height)
        }

        fun filterBlocks(): Function<Flux<Block>, Flux<Block>> {
            return Function { all ->
                all
                        .flatMap({ block -> isOnMain(block).map { Tuples.of(it, block) } }, 1)
                        .filter { it.t1 }
                        .map { it.t2 }
            }
        }

        fun filterTxes(): Function<Flux<Transaction>, Flux<Transaction>> {
            return Function { all ->
                all
                        .flatMap({ block -> isOnMain(block).map { Tuples.of(it, block) } }, 1)
                        .filter { it.t1 }
                        .map { it.t2 }
            }
        }

    }
}