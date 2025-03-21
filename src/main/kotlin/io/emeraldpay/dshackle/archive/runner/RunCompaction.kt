package io.emeraldpay.dshackle.archive.runner

import io.emeraldpay.dshackle.archive.BlocksRange
import io.emeraldpay.dshackle.archive.FileType
import io.emeraldpay.dshackle.archive.avro.Block
import io.emeraldpay.dshackle.archive.avro.Transaction
import io.emeraldpay.dshackle.archive.config.RunConfig
import io.emeraldpay.dshackle.archive.model.Chunk
import io.emeraldpay.dshackle.archive.storage.BlocksReader
import io.emeraldpay.dshackle.archive.storage.CompleteWriter
import io.emeraldpay.dshackle.archive.storage.ConfiguredFilenameGenerator
import io.emeraldpay.dshackle.archive.storage.FilenameGenerator
import io.emeraldpay.dshackle.archive.storage.SourceStorage
import io.emeraldpay.dshackle.archive.storage.TargetStorage
import io.emeraldpay.dshackle.archive.storage.TransactionsReader
import org.reactivestreams.Publisher
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import reactor.util.function.Tuples
import java.net.URI
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap
import java.util.concurrent.locks.ReentrantLock
import java.util.function.Function
import kotlin.concurrent.withLock
import kotlin.io.path.name
import kotlin.system.exitProcess

@Service
@Profile("run-compact")
class RunCompaction(
    @Autowired private val completeWriter: CompleteWriter,
    @Autowired private val runConfig: RunConfig,
    @Autowired private val blocksRange: BlocksRange,
    @Autowired private val filenameGenerator: FilenameGenerator,
    @Autowired private val configuredFilenameGenerator: ConfiguredFilenameGenerator,
    @Autowired private val sourceStorage: SourceStorage,
    @Autowired private val targetStorage: TargetStorage,
    @Autowired private val transactionsReader: TransactionsReader,
    @Autowired private val blocksReader: BlocksReader,
    @Autowired(required = false) private val blockSource: BlockSource?,
) : RunCommand {

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
                log.warn(
                    "Compaction is configured to pack only non-forked blocks which requires active connection to the original blockchain. " +
                        "It's not provided for the current run and Dshackle Archive cannot continue without it. " +
                        "To disable such a verification and accept all data including forks, use option --compact.forks",
                )
                exitProcess(1)
            }
            val filters = ForkFilter(blockSource)
            filterBlocks = filters.filterBlocks()
            filterTxes = filters.filterTxes()
        }
    }

    override fun run(): Mono<Void> {
        log.info("Compact files")
        if (runConfig.inputFiles == null) {
            log.warn("List of input files is not set")
            return Mono.empty()
        }

        val sources = sourceStorage.getInputFiles()

        return Flux.merge(
            processBlocks(sources.blocks).subscribeOn(Schedulers.boundedElastic()),
            processTransactions(sources.transactions).subscribeOn(Schedulers.boundedElastic()),
        )
            .then(completeWriter.closeOpenFiles())
            .then()
    }

    fun processBlocks(files: Flux<Path>): Mono<Void> {
        return ProcessHelper(FileType.BLOCKS)
            .processFiles(
                files,
                { path ->
                    val input = sourceStorage.current.createReader(path)
                    blocksReader.open(input)
                },
                { chunk, entries ->
                    completeWriter
                        .consumeBlocksChunk(
                            chunk,
                            entries.transform(filterBlocks)
                                .filter { chunk.includes(it.height) }
                                .doOnError {
                                    log.error("Failed to read blocks chunk {}", chunk)
                                },
                        )
                        .then()
                },
            )
    }

    fun processTransactions(files: Flux<Path>): Mono<Void> {
        return ProcessHelper(FileType.TRANSACTIONS)
            .processFiles(
                files,
                { path ->
                    val input = sourceStorage.current.createReader(path)
                    transactionsReader.open(input)
                },
                { chunk, entries ->
                    completeWriter
                        .consumeTransactionsChunk(
                            chunk,
                            entries.transform(filterTxes)
                                .filter { chunk.includes(it.height) }
                                .doOnError {
                                    log.error("Failed to read transaction chunk {}", chunk)
                                },
                        )
                        .then()
                },
            )
    }

    data class ChunkedPath(
        val chunk: Chunk,
        val path: Path,
    )

    data class ChunkedPaths(
        val chunk: Chunk,
        val paths: List<Path>,
    )

    private fun rechunkByActualBlocks(
        initialChunk: Chunk,
        list: List<Path>,
        isFirst: Boolean,
        isLast: Boolean,
    ): List<ChunkedPaths> {
        val sortedByStartBlock = list
            .map {
                val fileChunk = filenameGenerator.parseRange(it.fileName.name)!!
                ChunkedPath(fileChunk, it)
            }
            .sortedBy { it.chunk.startBlock }

        val mergedChunks = mutableListOf<ChunkedPaths>()
        var mergedChunk: Chunk? = null
        var mergedChunkPaths = mutableListOf<Path>()

        // while processing ranges it is possible then range file contains block before and after
        // the while chunk,
        // they should be processed as separate chunks to avoid block loss
        var preChunk: Chunk? = null
        var preChunkPaths = mutableListOf<Path>()

        var postChunk: Chunk? = null
        var postChunkPaths = mutableListOf<Path>()

        for (path in sortedByStartBlock) {
            if (isFirst && path.chunk.startBlock < initialChunk.startBlock) {
                preChunk = path.chunk
                preChunkPaths.add(path.path)
            }
            if (isLast && path.chunk.endBlock > initialChunk.endBlock) {
                postChunk = path.chunk
                postChunkPaths.add(path.path)
            }
            if (mergedChunk == null) {
                mergedChunk = path.chunk
                mergedChunkPaths = mutableListOf(path.path)
            } else {
                // if the next chunk connects merged chunk, join them
                if (mergedChunk.endBlock + 1 >= path.chunk.startBlock) {
                    mergedChunk = mergedChunk.join(path.chunk)
                    mergedChunkPaths.add(path.path)
                } else {
                    // cut chunk by initial chunk range
                    val cutChunk = initialChunk.intersection(mergedChunk)
                    mergedChunks.add(ChunkedPaths(cutChunk, mergedChunkPaths))
                    mergedChunk = path.chunk
                    mergedChunkPaths = mutableListOf(path.path)
                }
            }
        }
        if (mergedChunk != null) {
            val cutChunk = initialChunk.intersection(mergedChunk)
            mergedChunks.add(ChunkedPaths(cutChunk, mergedChunkPaths))
        }
        if (preChunk != null) {
            val cutChunk = Chunk.between(preChunk.startBlock, initialChunk.startBlock - 1)
            mergedChunks.add(ChunkedPaths(cutChunk, preChunkPaths))
        }
        if (postChunk != null) {
            val cutChunk = Chunk.between(initialChunk.endBlock + 1, postChunk.endBlock)
            mergedChunks.add(ChunkedPaths(cutChunk, postChunkPaths))
        }
        return mergedChunks
    }

    /** Group source files into flux of files per chunk */
    fun groupByChunk(files: Flux<Path>): Flux<ChunkedPaths> {
        val chunks = blocksRange.getChunks()
        val wholeChunk = blocksRange.wholeChunk()
        var current: ChunkedPaths? = null
        return files
            .filter {
                val isSingle = filenameGenerator.isSingle(it.fileName.name)
                if (isSingle || runConfig.compaction.compactRanges) {
                    val fileChunk = filenameGenerator.parseRange(it.fileName.name)
                    fileChunk != null && wholeChunk.intersects(fileChunk)
                } else {
                    false
                }
            }
            .handle { next, sink ->
                // files come as sorted by name, so we know when the next chunk started
                //
                val fileChunk = filenameGenerator.parseRange(next.fileName.name)!!
                val chunk = chunks.find { it.intersects(fileChunk) } ?: return@handle
                current = if (current == null) {
                    ChunkedPaths(chunk, listOf(next))
                } else {
                    if (current!!.chunk.intersects(chunk)) {
                        // add to the current chunk and continue
                        ChunkedPaths(chunk, current!!.paths + listOf(next))
                    } else {
                        // a new chunk started, emit current
                        sink.next(current!!)
                        ChunkedPaths(chunk, listOf(next))
                    }
                }
            }
            .concatWith(
                Mono.fromCallable { current }.flatMapMany { Mono.justOrEmpty(it) },
            )
    }

    class FileReferenceCounter {
        val map: ConcurrentMap<Path, Set<Chunk>> = ConcurrentHashMap()

        fun push(chunk: Chunk, files: List<Path>) {
            for (file in files) {
                push(file, chunk)
            }
        }

        fun push(file: Path, chunk: Chunk) {
            map.compute(file) { _, v ->
                if (v == null) {
                    setOf(chunk)
                } else {
                    v + chunk
                }
            }
        }

        /**
         * @return true, if the last chunk removed
         */
        fun removeAndCheckIfEmpty(file: Path, chunk: Chunk): Boolean {
            var oldValue: Chunk? = null
            val set = map.compute(file) { _, v ->
                if (v == null) {
                    null
                } else {
                    if (v.contains(chunk)) {
                        oldValue = chunk
                        val updated = v - chunk
                        updated.ifEmpty { null }
                    } else {
                        v
                    }
                }
            }
            return oldValue != null && set.isNullOrEmpty()
        }
    }

    inner class ProcessHelper(
        private val fileType: FileType,
    ) {
        private val fileReferenceCounter = FileReferenceCounter()

        fun <T> processFiles(
            files: Flux<Path>,
            read: (Path) -> Publisher<T>,
            write: (Chunk, Flux<T>) -> Mono<Void>,
        ): Mono<Void> {
            val groups = groupByChunk(files)
            val wholeChunk = blocksRange.wholeChunk()

            return groups
                .flatMap { group ->
                    val groupChunk = group.chunk
                    Flux.fromIterable(
                        rechunkByActualBlocks(
                            initialChunk = groupChunk,
                            list = group.paths,
                            isFirst = groupChunk.startBlock == wholeChunk.startBlock,
                            isLast = groupChunk.endBlock == wholeChunk.endBlock,
                        ),
                    )
                }
                .doOnNext { group ->
                    fileReferenceCounter.push(group.chunk, group.paths)
                }
                .flatMap {
                    processChunk(
                        it.chunk,
                        Flux.fromIterable(it.paths),
                        read,
                    ) { entries ->
                        write(it.chunk, entries)
                    }
                }
                .then()
        }

        private fun <T> processChunk(
            chunk: Chunk,
            chunkInputs: Flux<Path>,
            read: (Path) -> Publisher<T>,
            write: (Flux<T>) -> Mono<Void>,
        ): Mono<Void> {
            val chunkFile = configuredFilenameGenerator.fileFor(fileType, chunk)
            val chunkFileUri = URI.create(targetStorage.current.getURI(chunkFile))
            // here we track all processed files so we can delete them later
            val consumed = mutableListOf<Path>()
            val consumedFlux = chunkInputs.doOnNext {
                // filter file if it is actually target file to prevent deleting
                if (it.toUri() != chunkFileUri) {
                    consumed.add(it)
                }
            }
            val writeFlux = if (targetStorage.current.exists(chunkFile)) {
                log.debug("Chunk file $chunkFile already exists, skipping")
                // remove reference to prevent target file deleting
                fileReferenceCounter.removeAndCheckIfEmpty(Paths.get(chunkFileUri), chunk)
                consumedFlux.then()
            } else {
                write(
                    consumedFlux.flatMapSequential(read),
                )
            }

            return writeFlux
                .then(
                    // use Callable to process the consumed files only at the last step, otherwise
                    // it may be incomplete
                    Mono.fromCallable {
                        consumed.mapNotNull {
                            if (fileReferenceCounter.removeAndCheckIfEmpty(it, chunk)) {
                                it
                            } else {
                                null
                            }
                        }
                    }
                        .filter { it.isNotEmpty() }
                        .flatMap { paths ->
                            if (!runConfig.dryRun) {
                                log.info("Deleting files: ${paths.joinToString(", ")}")
                                Mono
                                    .fromCallable {
                                        paths.forEach {
                                            Files.deleteIfExists(it)
                                        }
                                    }
                                    .subscribeOn(Schedulers.boundedElastic())
                            } else {
                                log.info("DRY RUN! Deleting files: ${paths.joinToString(", ")}")
                                Mono.empty()
                            }
                        },
                )
                .then()
        }
    }

    class ForkFilter(
        private val blockSource: BlockSource,
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
