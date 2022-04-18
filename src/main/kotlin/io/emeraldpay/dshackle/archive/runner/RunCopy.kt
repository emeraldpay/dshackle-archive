package io.emeraldpay.dshackle.archive.runner

import io.emeraldpay.dshackle.archive.BlocksRange
import io.emeraldpay.dshackle.archive.config.RunConfig
import io.emeraldpay.dshackle.archive.storage.fs.BlocksReader
import io.emeraldpay.dshackle.archive.storage.CompleteWriter
import io.emeraldpay.dshackle.archive.storage.FilenameGenerator
import io.emeraldpay.dshackle.archive.storage.fs.TransactionsReader
import java.nio.file.Files
import java.nio.file.Path
import java.util.function.Consumer
import kotlin.io.path.name
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux

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

        val transactions = ArrayList<Path>()
        val blocks = ArrayList<Path>()
        val collect = Consumer<Path> {
            if (it.name.contains("transactions") || it.name.contains("txes")) {
                transactions.add(it)
            } else if (it.name.contains("blocks") || it.name.contains("block")) {
                blocks.add(it)
            } else {
                log.warn("Unknown type of file: $it")
            }
        }
        val range = blocksRange.wholeChunk()
        runConfig.inputFiles.files.forEach { pattern ->
            if (pattern.contains("*")) {
                val dir = Path.of(pattern.substringBeforeLast("/"))
                val glob = pattern.substringAfterLast("/")
                log.info("Read files at $dir with $glob")
                Files.newDirectoryStream(dir, glob)
                        .filter { file ->
                            val chunk = filenameGenerator.parseRange(file.fileName.name)
                            if (chunk == null) {
                                log.debug("Skip ${file.fileName}")
                            }
                            val accept = chunk != null && range.intersects(chunk)
                            if (!accept) {
                                log.trace("Skip ${file.fileName}")
                            }
                            accept
                        }
                        .sortedBy { file ->
                            val chunk = filenameGenerator.parseRange(file.fileName.name)
                            chunk!!.startBlock
                        }
                        .forEach(collect)
            } else {
                collect.accept(Path.of(pattern))
            }
        }

        log.info("Copy blocks")
        copyBlocks(blocks)
        log.info("Copy transactions")
        copyTransactions(transactions)
    }

    fun copyBlocks(files: Iterable<Path>) {
        blocksRange.getChunks().forEach { chunk ->
        }
        val source = Flux.fromIterable(files)
                .flatMap { file ->
                    BlocksReader().open(file)
                }
                .filter {
                    blocksRange.includes(it.height)
                }
        completeWriter.consumeBlocks(source, null)
    }

    fun copyTransactions(files: Iterable<Path>) {
        val source = Flux.fromIterable(files)
                .flatMap { file ->
                    TransactionsReader().open(file)
                }
                .filter {
                    blocksRange.includes(it.height)
                }
        completeWriter.consumeTransactions(source, null)
    }

}