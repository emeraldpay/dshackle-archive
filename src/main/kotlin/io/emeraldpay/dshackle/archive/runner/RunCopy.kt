package io.emeraldpay.dshackle.archive.runner

import io.emeraldpay.dshackle.archive.BlocksRange
import io.emeraldpay.dshackle.archive.config.RunConfig
import io.emeraldpay.dshackle.archive.storage.CompleteWriter
import io.emeraldpay.dshackle.archive.storage.SourceStorage
import io.emeraldpay.dshackle.archive.storage.BlocksReader
import io.emeraldpay.dshackle.archive.storage.TransactionsReader
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import java.nio.file.Path
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import java.util.function.Function

@Service
@Profile("run-copy")
class RunCopy(
        @Autowired private val completeWriter: CompleteWriter,
        @Autowired private val runConfig: RunConfig,
        @Autowired private val blocksRange: BlocksRange,
        @Autowired private val sourceStorage: SourceStorage,
        @Autowired private val transactionsReader: TransactionsReader,
        @Autowired private val blocksReader: BlocksReader,
) : RunCommand {

    companion object {
        private val log = LoggerFactory.getLogger(RunCopy::class.java)
    }

    override fun run(): Mono<Void> {
        log.info("Recover files")
        if (runConfig.inputFiles == null) {
            log.warn("List of input files is not set")
            return Mono.empty()
        }

        val sources = sourceStorage.getInputFiles()

        return Mono.zip(
                processBlocks(sources.blocks)
                        .thenReturn(true)
                        .subscribeOn(Schedulers.boundedElastic()),

                processTransactions(sources.transactions)
                        .thenReturn(true)
                        .subscribeOn(Schedulers.boundedElastic())
        ).then(completeWriter.closeOpenFiles()).then()
    }

    fun processBlocks(files: Flux<Path>): Mono<Void> {
        // not it's overriden for compactions
        val source = files
                .doOnSubscribe { log.info("Process blocks") }
                .flatMap(blocksReader::open)
                .filter {
                    blocksRange.includes(it.height)
                }
        return completeWriter.consumeBlocks(source).then()
    }

    fun processTransactions(files: Flux<Path>): Mono<Void> {
        // not it's overriden for compactions
        val source = files
                .doOnSubscribe { log.info("Process transactions") }
                .flatMap(transactionsReader::open)
                .filter {
                    blocksRange.includes(it.height)
                }
        return completeWriter.consumeTransactions(source).then()
    }

}