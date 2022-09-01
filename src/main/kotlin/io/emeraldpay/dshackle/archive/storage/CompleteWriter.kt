package io.emeraldpay.dshackle.archive.storage

import io.emeraldpay.dshackle.archive.FileType
import io.emeraldpay.dshackle.archive.avro.Block
import io.emeraldpay.dshackle.archive.avro.Transaction
import io.emeraldpay.dshackle.archive.model.Chunk
import io.emeraldpay.dshackle.archive.notify.CurrentNotifier
import io.emeraldpay.dshackle.archive.runner.ProgressIndicator
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
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
        //NOTE rewrites the files
        val blockFile = configuredFilenameGenerator.fileFor(FileType.BLOCKS, chunk)
        val blockWrt = blocksWriter.open(blockFile)
        val txFile = configuredFilenameGenerator.fileFor(FileType.TRANSACTIONS, chunk)
        val txWrt = transactionsWriter.open(txFile)
        val saving = dataSource
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
                .doOnNext { count ->
                    log.info("$count blocks saved")
                }

        val notifyBlock = currentNotifier.onCreated(
                currentNotifier.createEvent(
                        FileType.BLOCKS, chunk, blockFile
                )
        )
        val notifyTxes = currentNotifier.onCreated(
                currentNotifier.createEvent(
                        FileType.TRANSACTIONS, chunk, txFile
                )
        )
        val notifyAll = Flux.merge(notifyBlock, notifyTxes).then()

        return saving.then(notifyAll)
    }

    fun consumeBlocks(dataSource: Flux<Block>) : Mono<Int> {
        val progress = ProgressIndicator()
        val startTime = System.currentTimeMillis()
        return dataSource
                .doOnSubscribe {
                    progress.start()
                }
                .flatMapSequential ({ block ->
                    Mono.fromCallable {
                        val blockFile = configuredFilenameGenerator.fileForAutoRange(FileType.BLOCKS, block.height)
                        blocksWriter.open(blockFile).append(block)
                        1
                    }
                }, 4)
                .doOnNext {
                    progress.onNext()
                }
                .reduce { a, b ->
                    val total = a + b
                    if (total % 10000 == 0) {
                        log.debug("Processed $total blocks")
                    }
                    total
                }
                .doFinally {
                    val time = System.currentTimeMillis() - startTime
                    val minutes = time / 60_000
                    val seconds = (time % 60_000) / 1000
                    log.info("Archived in ${minutes}m:${StringUtils.leftPad(seconds.toString(), 2, "0")}s")
                }
    }

    fun consumeTransactions(dataSource: Flux<Transaction>) : Mono<Int> {
        val progress = ProgressIndicator()
        val startTime = System.currentTimeMillis()
        return dataSource
                .doOnSubscribe {
                    progress.start()
                }
                .flatMapSequential ({ tx ->
                    Mono.fromCallable {
                        // note that it most likely appends to an existing file
                        val txesFile = configuredFilenameGenerator.fileForAutoRange(FileType.TRANSACTIONS, tx.height)
                        transactionsWriter.open(txesFile).append(tx)
                        1
                    }
                }, 4)
                .doOnNext {
                    progress.onNext()
                }
                .reduce { a, b ->
                    val total = a + b
                    if (total % 100000 == 0) {
                        log.debug("Processed $total transactions")
                    }
                    total
                }
                .doFinally {
                    val time = System.currentTimeMillis() - startTime
                    val minutes = time / 60_000
                    val seconds = (time % 60_000) / 1000
                    log.info("Archived in ${minutes}m:${StringUtils.leftPad(seconds.toString(), 2, "0")}s")
                }
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
                                    val txesFile = configuredFilenameGenerator.fileForAutoRange(FileType.TRANSACTIONS, block.height)
                                    transactionsWriter.open(txesFile).use { wrt ->
                                        block.transactions.forEach { tx ->
                                            wrt.append(block, tx)
                                        }
                                    }
                                    val blockFile = configuredFilenameGenerator.fileForAutoRange(FileType.BLOCKS, block.height)
                                    blocksWriter.open(blockFile).use { wrt ->
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

    /**
     * Runs the created file through current Notifier.
     * Expects a pair of <Path to Transactions> and <Path to Blocks> as input
     */
    fun withNotifications(block: BlockDetails): java.util.function.Function<Mono<Tuple2<String, String>>, Mono<Void>> {
        return java.util.function.Function { files ->
            files.flatMap {
                val txnotify = currentNotifier.onCreated(
                        currentNotifier.createEvent(
                                FileType.TRANSACTIONS, block.height, block.height, targetStorage.current.getURI(it.t1)
                        )
                )
                val blocknotify = currentNotifier.onCreated(
                        currentNotifier.createEvent(
                                FileType.BLOCKS, block.height, block.height, targetStorage.current.getURI(it.t2)
                        )
                )
                txnotify.then(blocknotify).then()
            }
        }
    }

}