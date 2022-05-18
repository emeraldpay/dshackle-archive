package io.emeraldpay.dshackle.archive.runner

import io.emeraldpay.dshackle.archive.BlocksRange
import io.emeraldpay.dshackle.archive.config.RunConfig
import io.emeraldpay.dshackle.archive.storage.CompleteWriter
import io.emeraldpay.dshackle.archive.storage.FilenameGenerator
import io.emeraldpay.dshackle.archive.storage.SourceStorage
import io.emeraldpay.dshackle.archive.storage.fs.BlocksReader
import io.emeraldpay.dshackle.archive.storage.fs.TransactionsReader
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import java.nio.file.Path
import kotlin.io.path.name
import org.reactivestreams.Publisher
import reactor.core.publisher.GroupedFlux
import reactor.core.publisher.Mono

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
) : RunCopy(completeWriter, runConfig, blocksRange, sourceStorage, transactionsReader, blocksReader) {

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
    fun groupByChunk(files: Flux<Path>): Flux<GroupedFlux<BlocksRange.Chunk, Path>> {
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
            completeWriter.consumeTransactions(it).then()
        }
    }

    fun openAndConsumeBlocks(chunkInputs: Flux<Path>): Mono<Void> {
        return processChunk(chunkInputs, blocksReader::open) {
            completeWriter.consumeBlocks(it).then()
        }
    }

    fun <T> processChunk(chunkInputs: Flux<Path>, open: (Path) -> Publisher<T>, accept: (Flux<T>) -> Mono<Void>): Mono<Void> {
        // here we track all processed files so we can delete them later
        val consumed = mutableListOf<Path>()
        val source = chunkInputs
                // remember the file
                .doOnNext(consumed::add)
                .flatMap(open)

        return accept(source)
                .then(
                        // use Callable to process the consumed files only at the last step, otherwise it may be incomplete
                        Mono.fromCallable { consumed.map { it.toFile().path } }
                                .flatMap(sourceStorage::deleteArchives)
                )
                .then()
    }

}