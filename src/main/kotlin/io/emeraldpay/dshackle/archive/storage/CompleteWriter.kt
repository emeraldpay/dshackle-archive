package io.emeraldpay.dshackle.archive.storage

import io.emeraldpay.dshackle.archive.avro.Block
import io.emeraldpay.dshackle.archive.avro.Transaction
import io.emeraldpay.dshackle.archive.BlocksRange
import io.emeraldpay.dshackle.archive.runner.PostArchive
import io.emeraldpay.dshackle.archive.runner.ProgressIndicator
import io.emeraldpay.dshackle.archive.storage.fs.BlocksWriter
import io.emeraldpay.dshackle.archive.storage.fs.TransactionsWriter
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Repository
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.nio.file.Files
import java.nio.file.Path

@Repository
class CompleteWriter(
        @Autowired private val blocksWriter: BlocksWriter,
        @Autowired private val transactionsWriter: TransactionsWriter,
        @Autowired private val configuredFilenameGenerator: ConfiguredFilenameGenerator,
        @Autowired private val postArchive: PostArchive
) {

    companion object {
        private val log = LoggerFactory.getLogger(CompleteWriter::class.java)

        private const val FILE_TYPE_TXES = "txes"
        private const val FILE_TYPE_BLOCKS = "blocks"
        private const val FILE_TYPE_BLOCK = "block"
    }

    fun consume(dataSource: Flux<BlockDetails>, chunk: BlocksRange.Chunk) {
        //NOTE rewrites the files
        val blockFile = configuredFilenameGenerator.fileFor(FILE_TYPE_BLOCKS, chunk, false)
        val blockWrt = blocksWriter.open(blockFile)
        val txFile = configuredFilenameGenerator.fileFor(FILE_TYPE_TXES, chunk, false)
        val txWrt = transactionsWriter.open(txFile)
        val count = dataSource
                .map { block ->
                    block.transactions.forEach { tx ->
                        txWrt.append(block, tx)
                    }
                    blockWrt.append(block)
                    1
                }
                .doFinally {
                    txWrt.close()
                    blockWrt.close()
                }
                .doOnError { t ->
                    log.error("Failed to export", t)
                }
                .reduce { a, b ->
                    val total = a + b
                    if (total % 100 == 0) {
                        log.debug("Processed $total blocks")
                    }
                    total
                }
                .block()
        log.info("$count blocks saved")
        postArchive.handle(blockFile)
        postArchive.handle(txFile)
    }

    fun consumeBlocks(dataSource: Flux<Block>, filesToDelete : ArrayList<Path>?) {
        val progress = ProgressIndicator()
        val startTime = System.currentTimeMillis()
        dataSource
                .doOnSubscribe {
                    progress.start()
                }
                .flatMap({ block ->
                    Mono.fromCallable {
                        val blockFile = configuredFilenameGenerator.fileForAutoRange(FILE_TYPE_BLOCK, block.height, false)
                        blocksWriter.open(blockFile).append(block)
                    }
                }, 4)
                .doOnNext {
                    progress.onNext()
                }
                .map { 1 }
                .reduce { a, b ->
                    val total = a + b
                    if (total % 10000 == 0) {
                        log.debug("Processed $total blocks")
                    }
                    total
                }
                .doFinally {
                    blocksWriter.closeAll()
                    filesToDelete?.stream()?.forEach { Files.deleteIfExists(it) }
                    val time = System.currentTimeMillis() - startTime
                    val minutes = time / 60_000
                    val seconds = (time % 60_000) / 1000
                    log.info("Archived in ${minutes}m:${StringUtils.leftPad(seconds.toString(), 2, "0")}s")
                }
                .block()
    }

    fun consumeTransactions(dataSource: Flux<Transaction>, filesToDelete: ArrayList<Path>?) {
        val progress = ProgressIndicator()
        val startTime = System.currentTimeMillis()
        dataSource
                .doOnSubscribe {
                    progress.start()
                }
                .flatMap({ tx ->
                    Mono.fromCallable {
                        val txesFile = configuredFilenameGenerator.fileForAutoRange(FILE_TYPE_TXES, tx.height, true)
                        transactionsWriter.open(txesFile).append(tx)
                    }
                }, 4)
                .doOnNext {
                    progress.onNext()
                }
                .map { 1 }
                .reduce { a, b ->
                    val total = a + b
                    if (total % 100000 == 0) {
                        log.debug("Processed $total transactions")
                    }
                    total
                }
                .doFinally {
                    blocksWriter.closeAll()
                    filesToDelete?.stream()?.forEach { Files.deleteIfExists(it) }
                    val time = System.currentTimeMillis() - startTime
                    val minutes = time / 60_000
                    val seconds = (time % 60_000) / 1000
                    log.info("Archived in ${minutes}m:${StringUtils.leftPad(seconds.toString(), 2, "0")}s")
                }
                .block()
    }

    fun streamConsumer(): java.util.function.Function<Flux<BlockDetails>, Flux<Long>> {
        return java.util.function.Function { dataSource ->
            val progress = ProgressIndicator()
            val startTime = System.currentTimeMillis()
            dataSource
                    .doOnSubscribe {
                        progress.start()
                    }
                    .flatMap({ block ->
                        Mono.fromCallable {
                            val txesFile = configuredFilenameGenerator.fileForAutoRange(FILE_TYPE_TXES, block.height, false)
                            transactionsWriter.open(txesFile).use { wrt ->
                                block.transactions.forEach { tx ->
                                    wrt.append(block, tx)
                                }
                            }
                            val blockFile = configuredFilenameGenerator.fileForAutoRange(FILE_TYPE_BLOCK, block.height, false)
                            blocksWriter.open(blockFile).use { wrt ->
                                wrt.append(block)
                            }

                            postArchive.handle(txesFile)
                            postArchive.handle(blockFile)
                        }.then(Mono.just(block.height)).onErrorResume { t ->
                            log.warn("Failed to process ${block.height}", t)
                            Mono.empty<Long>()
                        }
                    }, 4)
                    .doOnNext {
                        progress.onNext()
                    }
                    .doFinally {
                        val time = System.currentTimeMillis() - startTime
                        val minutes = time / 60_000
                        val seconds = (time % 60_000) / 1000
                        log.info("Archived in ${minutes}m:${StringUtils.leftPad(seconds.toString(), 2, "0")}s")
                    }
        }
    }


}