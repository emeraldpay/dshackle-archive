package io.emeraldpay.dshackle.archive.runner

import io.emeraldpay.dshackle.archive.BlocksRange
import io.emeraldpay.dshackle.archive.config.RunConfig
import io.emeraldpay.dshackle.archive.storage.CompleteWriter
import io.emeraldpay.dshackle.archive.storage.FilenameGenerator
import io.emeraldpay.dshackle.archive.storage.fs.BlocksReader
import io.emeraldpay.dshackle.archive.storage.fs.TransactionsReader
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import java.io.File
import java.nio.file.Path
import kotlin.io.path.name

@Service
@Profile("run-copy")
class RunCopy(
        @Autowired private val completeWriter: CompleteWriter,
        @Autowired private val runConfig: RunConfig,
        @Autowired private val blocksRange: BlocksRange,
        @Autowired private val filenameGenerator: FilenameGenerator
) : Runnable {

    companion object {
        private val log = LoggerFactory.getLogger(RunCopy::class.java)
    }

    override fun run() {
        log.info("Recover files")
        if (runConfig.inputFiles == null) {
            log.warn("List of input files is not set")
            return
        }

        val transactions = mutableListOf<Path>()
        val blocks = mutableListOf<Path>()
        val range = blocksRange.wholeChunk()
        runConfig.inputFiles.files.forEach { pattern ->
            File(pattern).walk()
                .filter { file ->
                    val chunk = filenameGenerator.parseRange(file.name)
                    if (chunk == null) {
                        log.debug("Skip ${file.name}")
                    }
                    val accept = chunk != null && range.intersects(chunk)
                    if (!accept) {
                        log.trace("Skip ${file.name}")
                    }
                    accept
                }
                .sortedBy { file ->
                    val chunk = filenameGenerator.parseRange(file.name)
                    chunk!!.startBlock
                }
                .map { it.toPath() }
                .forEach {
                    if (it.name.contains("transactions") || it.name.contains("txes")) {
                        transactions.add(it)
                    } else if (it.name.contains("blocks") || it.name.contains("block")) {
                        blocks.add(it)
                    } else {
                        log.warn("Unknown type of file: $it")
                    }
                }
        }

        log.info("Process blocks")
        processBlocks(blocks)
        log.info("Process transactions")
        processTransactions(transactions)
    }

    fun processBlocks(files: Iterable<Path>) {
        blocksRange.getChunks().forEach { chunk ->
        }
        val source = Flux.fromIterable(files)
                .flatMap { file ->
                    BlocksReader().open(file)
                }
                .filter {
                    blocksRange.includes(it.height)
                }
        completeWriter.consumeBlocks(source).block()
    }

    fun processTransactions(files: Iterable<Path>) {
        val source = Flux.fromIterable(files)
                .flatMap { file ->
                    TransactionsReader().open(file)
                }
                .filter {
                    blocksRange.includes(it.height)
                }
        completeWriter.consumeTransactions(source).block()
    }

}