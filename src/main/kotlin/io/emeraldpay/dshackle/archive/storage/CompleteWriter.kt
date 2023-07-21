package io.emeraldpay.dshackle.archive.storage

import io.emeraldpay.dshackle.archive.FileType
import io.emeraldpay.dshackle.archive.avro.Block
import io.emeraldpay.dshackle.archive.avro.Transaction
import io.emeraldpay.dshackle.archive.config.RunConfig
import io.emeraldpay.dshackle.archive.model.Chunk
import io.emeraldpay.dshackle.archive.notify.CurrentNotifier
import io.emeraldpay.dshackle.archive.runner.ProgressIndicator
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Repository
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import reactor.util.function.Tuple2
import reactor.util.function.Tuples

@Profile("!run-report")
@Repository
class CompleteWriter(
    @Autowired private val blocksWriter: BlocksWriter,
    @Autowired private val transactionsWriter: TransactionsWriter,
    @Autowired private val configuredFilenameGenerator: ConfiguredFilenameGenerator,
    @Autowired private val currentNotifier: CurrentNotifier,
    @Autowired private val targetStorage: TargetStorage,
    @Autowired private val runConfig: RunConfig,
) {

    companion object {
        private val log = LoggerFactory.getLogger(CompleteWriter::class.java)
    }

    fun closeOpenFiles(): Mono<Void> {
        return Mono.fromCallable {
            blocksWriter.closeAll()
            transactionsWriter.closeAll()
        }.then()
    }

    fun consume(dataSource: Flux<BlockDetails>, chunk: Chunk): Mono<Void> {
        val progress = ProgressIndicator(opLabel = "blocks")
        // NOTE rewrites the files
        val blockFile = configuredFilenameGenerator.fileFor(FileType.BLOCKS, chunk)
        val blockWrt = blocksWriter.openShared(blockFile)
        val txFile = configuredFilenameGenerator.fileFor(FileType.TRANSACTIONS, chunk)
        val txWrt = transactionsWriter.openShared(txFile)
        val saving = dataSource
            .doOnSubscribe {
                progress.start()
            }
            .flatMap(
                { block ->
                    Mono.zip(
                        Mono.fromCallable {
                            block.transactions.forEach { tx ->
                                txWrt.append(block, tx)
                            }
                            1
                        }.subscribeOn(Schedulers.boundedElastic()),
                        Mono.fromCallable {
                            blockWrt.append(block)
                            1
                        }.subscribeOn(Schedulers.boundedElastic()),
                    ).then(Mono.just(1))
                },
                1,
            )
            .doOnNext {
                progress.onNext()
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
            .doOnNext { count ->
                log.info("$count blocks saved")
            }

        val notifyBlock = currentNotifier.onCreated(
            currentNotifier.createEvent(
                FileType.BLOCKS,
                chunk,
                blockFile,
            ),
        )
        val notifyTxes = currentNotifier.onCreated(
            currentNotifier.createEvent(
                FileType.TRANSACTIONS,
                chunk,
                txFile,
            ),
        )
        val notifyAll = Flux.merge(notifyBlock, notifyTxes).then()

        return saving.then(notifyAll)
    }

    fun consumeBlocks(dataSource: Flux<Block>): Mono<Int> {
        val progress = ProgressIndicator(opLabel = "blocks")
        val startTime = System.currentTimeMillis()
        return dataSource
            .doOnSubscribe {
                progress.start()
            }
            .flatMapSequential(
                { block ->
                    Mono.fromCallable {
                        val blockFile = configuredFilenameGenerator.fileForAutoRange(FileType.BLOCKS, block.height)
                        blocksWriter.openShared(blockFile).append(block)
                        1
                    }
                },
                4,
            )
            .doOnNext { progress.onNext() }
            .reduce(::logBlockProgress)
            .doFinally { logArchivedTime(startTime) }
    }

    fun consumeBlocksChunk(chunk: Chunk, blocks: Flux<Block>): Mono<Int> {
        val progress = ProgressIndicator(opLabel = "blocks")
        val startTime = System.currentTimeMillis()
        val chunkFile = configuredFilenameGenerator.fileFor(FileType.BLOCKS, chunk)
        if (blocksWriter.exists(chunkFile)) {
            log.info("Chunk file $chunkFile already exists, skipping")
            return Mono.just(0)
        }
        if (runConfig.dryRun) {
            log.info("DRY RUN! Save chunk $chunk blocks to $chunkFile")
            return Mono.just(0)
        }
        log.info("Save chunk $chunk blocks to $chunkFile")
        val writer = blocksWriter.openExclusively(chunkFile)
        return blocks
            .doOnSubscribe { progress.start() }
            .flatMapSequential(
                { block ->
                    Mono.fromCallable {
                        // blocking call
                        writer.append(block)
                        1
                    }
                },
                4,
            )
            .doOnNext { progress.onNext() }
            .reduce(::logBlockProgress)
            .doOnSuccess {
                writer.close()
                logArchivedTime(startTime, chunkFile)
            }
            .doOnError {
                log.error("Failed to write block chunk {}, {}", chunk, chunkFile)
            }
    }

    private fun logBlockProgress(a: Int, b: Int): Int {
        val total = a + b
        if (total % 10000 == 0) {
            log.debug("Processed $total blocks")
        }
        return total
    }

    fun consumeTransactions(dataSource: Flux<Transaction>): Mono<Int> {
        val progress = ProgressIndicator(opLabel = "transactions")
        val startTime = System.currentTimeMillis()
        return dataSource
            .doOnSubscribe {
                progress.start()
            }
            .flatMapSequential(
                { tx ->
                    Mono.fromCallable {
                        // note that it most likely appends to an existing file
                        val txesFile = configuredFilenameGenerator.fileForAutoRange(FileType.TRANSACTIONS, tx.height)
                        transactionsWriter.openShared(txesFile).append(tx)
                        1
                    }
                },
                4,
            )
            .doOnNext { progress.onNext() }
            .reduce(::logTransactionProgress)
            .doFinally { logArchivedTime(startTime) }
    }

    fun consumeTransactionsChunk(chunk: Chunk, dataSource: Flux<Transaction>): Mono<Int> {
        val progress = ProgressIndicator(opLabel = "transactions")
        val startTime = System.currentTimeMillis()
        val chunkFile = configuredFilenameGenerator.fileFor(FileType.TRANSACTIONS, chunk)
        if (transactionsWriter.exists(chunkFile)) {
            log.info("Chunk file $chunkFile already exists, skipping")
            return Mono.just(0)
        }
        if (runConfig.dryRun) {
            log.info("DRY RUN! Save chunk $chunk transactions to $chunkFile")
            return Mono.just(0)
        }
        log.info("Save chunk $chunk transactions to $chunkFile")

        val writer = transactionsWriter.openExclusively(chunkFile)
        return dataSource
            .doOnSubscribe { progress.start() }
            .flatMapSequential(
                { tx ->
                    Mono.fromCallable {
                        // note that it most likely appends to an existing file
                        writer.append(tx)
                        1
                    }
                },
                4,
            )
            .doOnNext { progress.onNext() }
            .reduce(::logTransactionProgress)
            .doOnSuccess {
                writer.close()
                logArchivedTime(startTime, chunkFile)
            }
            .doOnError {
                log.error("Failed to write transaction chunk {}, {}", chunk, chunkFile)
            }
    }

    private fun logTransactionProgress(a: Int, b: Int): Int {
        val total = a + b
        if (total % 100000 == 0) {
            log.debug("Processed $total transactions")
        }
        return total
    }

    fun streamConsumer(): java.util.function.Function<Flux<BlockDetails>, Flux<Long>> {
        return java.util.function.Function { dataSource ->
            val progress = ProgressIndicator()
            val startTime = System.currentTimeMillis()
            dataSource
                .doOnSubscribe {
                    progress.start()
                }
                .flatMap(
                    { block ->
                        Mono.fromCallable {
                            val txesFile = configuredFilenameGenerator.fileForAutoRange(FileType.TRANSACTIONS, block.height)
                            transactionsWriter.openShared(txesFile).use { wrt ->
                                block.transactions.forEach { tx ->
                                    wrt.append(block, tx)
                                }
                            }
                            val blockFile = configuredFilenameGenerator.fileForAutoRange(FileType.BLOCKS, block.height)
                            blocksWriter.openShared(blockFile).use { wrt ->
                                wrt.append(block)
                            }
                            Tuples.of(txesFile, blockFile)
                        }
                            .transform(withNotifications(block))
                            .then(Mono.just(block.height))
                            .onErrorResume { t ->
                                log.warn("Failed to process ${block.height}", t)
                                Mono.empty<Long>()
                            }
                    },
                    4,
                )
                .doOnNext { progress.onNext() }
                .doFinally { logArchivedTime(startTime) }
        }
    }

    private fun logArchivedTime(startTime: Long, file: String? = null) {
        val time = System.currentTimeMillis() - startTime
        val minutes = time / 60_000
        val seconds = (time % 60_000) / 1000
        log.info("Archived ${file?.plus(" ")}in ${minutes}m:${StringUtils.leftPad(seconds.toString(), 2, "0")}s")
    }

    /**
     * Runs the created file through current Notifier. Expects a pair of <Path to Transactions> and
     * <Path to Blocks> as input
     */
    fun withNotifications(block: BlockDetails): java.util.function.Function<Mono<Tuple2<String, String>>, Mono<Void>> {
        return java.util.function.Function { files ->
            files.flatMap {
                val txnotify = currentNotifier.onCreated(
                    currentNotifier.createEvent(
                        FileType.TRANSACTIONS,
                        block.height,
                        block.height,
                        targetStorage.current.getURI(it.t1),
                    ),
                )
                val blocknotify = currentNotifier.onCreated(
                    currentNotifier.createEvent(
                        FileType.BLOCKS,
                        block.height,
                        block.height,
                        targetStorage.current.getURI(it.t2),
                    ),
                )
                txnotify.then(blocknotify).then()
            }
        }
    }
}
