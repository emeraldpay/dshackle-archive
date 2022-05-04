package io.emeraldpay.dshackle.archive.runner

import io.emeraldpay.dshackle.archive.BlocksRange
import io.emeraldpay.dshackle.archive.config.RunConfig
import io.emeraldpay.dshackle.archive.storage.CompleteWriter
import io.emeraldpay.dshackle.archive.storage.FilenameGenerator
import io.emeraldpay.dshackle.archive.storage.fs.BlocksReader
import io.emeraldpay.dshackle.archive.storage.fs.TransactionsReader
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.name

@Service
@Profile("run-compact")
class RunCompaction(
    @Autowired private val completeWriter: CompleteWriter,
    @Autowired private val runConfig: RunConfig,
    @Autowired private val blocksRange: BlocksRange,
    @Autowired private val filenameGenerator: FilenameGenerator
) : RunCopy(completeWriter, runConfig, blocksRange, filenameGenerator) {

    override fun processBlocks(files: Iterable<Path>) {
        var groupOfSingles = mutableListOf<Path>()
        files.forEach {
            if (filenameGenerator.isSingle(it.fileName.name)) {
                groupOfSingles.add(it)
            } else if (groupOfSingles.isNotEmpty()) {
                openAndConsumeBlocks(groupOfSingles)
                groupOfSingles = mutableListOf()
            }
        }
        openAndConsumeBlocks(groupOfSingles)
    }

    override fun processTransactions(files: Iterable<Path>) {
        var groupOfSingles = mutableListOf<Path>()
        files.forEach {
            if (filenameGenerator.isSingle(it.fileName.name)) {
                groupOfSingles.add(it)
            } else if (groupOfSingles.isNotEmpty()) {
                openAndConsumeTransactions(groupOfSingles)
                groupOfSingles = mutableListOf()
            }
        }
        openAndConsumeTransactions(groupOfSingles)
    }

    fun openAndConsumeTransactions(groupOfSingles: MutableList<Path>) {
        val source = Flux.fromIterable(groupOfSingles)
            .flatMap { file ->
                TransactionsReader().open(file)
            }
        completeWriter.consumeTransactions(source)
            .doOnNext { groupOfSingles.forEach { Files.deleteIfExists(it) } }
            .block()
    }

    fun openAndConsumeBlocks(groupOfSingles: MutableList<Path>) {
        val source = Flux.fromIterable(groupOfSingles)
            .flatMap { file ->
                BlocksReader().open(file)
            }
        completeWriter.consumeBlocks(source)
            .doOnNext { groupOfSingles.forEach { Files.deleteIfExists(it) } }
            .block()
    }

}