package io.emeraldpay.dshackle.archive.runner

import io.emeraldpay.dshackle.archive.BlocksRange
import io.emeraldpay.dshackle.archive.FileType
import io.emeraldpay.dshackle.archive.config.RunConfig
import io.emeraldpay.dshackle.archive.model.BlockValidation
import io.emeraldpay.dshackle.archive.model.TransactionValidator
import io.emeraldpay.dshackle.archive.model.Validator
import io.emeraldpay.dshackle.archive.storage.AvroReader
import io.emeraldpay.dshackle.archive.storage.BlocksReader
import io.emeraldpay.dshackle.archive.storage.SourceStorage
import io.emeraldpay.dshackle.archive.storage.StorageAccess
import io.emeraldpay.dshackle.archive.storage.TransactionsReader
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import java.util.function.Function

@Service
@Profile("run-verify")
class RunVerify(
    @Autowired private val runConfig: RunConfig,
    @Autowired private val sourceStorage: SourceStorage,
    @Autowired private val scanningTools: ScanningTools,
    @Autowired private val rangeTools: RangeTools,
    @Autowired private val transactionsReader: TransactionsReader,
    @Autowired private val blocksReader: BlocksReader,
) : RunCommand, RunVerifyBase(sourceStorage.current) {

    companion object {
        private val log = LoggerFactory.getLogger(RunVerify::class.java)
    }

    private val transactionValidator = TransactionValidator.Builder()
        .forBlockchain(runConfig.chainType)
        .withOptions(runConfig.options)
        .build()
    private val blockValidator = BlockValidation.Builder()
        .forBlockchain(runConfig.chainType)
        .build()

    init {
        if (runConfig.dryRun) {
            super.dryRun = true
        }
    }

    override fun run(): Mono<Void> {
        return rangeTools.checkStartBlock()
            .map(BlocksRange::wholeChunk)
            .doOnNext { log.info("Verify blocks ${it.startBlock}..${it.endBlock}") }
            .flatMapMany(scanningTools::scanArchives)
            .flatMap(::processFile, 8)
            .doOnError { t -> log.error("Failed to verify archive", t) }
            .then()
            .doOnSubscribe { log.info("Verifying archive at ${runConfig.files.dir}") }
            .doFinally { log.info("Stopped...") }
    }

    fun processFile(file: ScanningTools.FileChunk): Mono<Void> {
        return getStatus(file)
            .transform(applyStatus(file))
            .subscribeOn(Schedulers.boundedElastic())
    }

    fun getStatus(file: ScanningTools.FileChunk): Mono<Status> {
        return when (file.type) {
            FileType.BLOCKS -> processWith(file, blocksReader, blockValidator)
            FileType.TRANSACTIONS -> processWith(file, transactionsReader, transactionValidator)
        }
    }
}

abstract class RunVerifyBase(
    private val storage: StorageAccess,
) {

    companion object {
        private val log = LoggerFactory.getLogger(RunVerify::class.java)
        private const val REPORT_PREFIX = "Check file"
    }

    protected var dryRun = false

    fun applyStatus(file: ScanningTools.FileChunk): Function<Mono<Status>, Mono<Void>> {
        return Function { status ->
            status.flatMap { status ->
                return@flatMap if (status.ok) {
                    log.debug("$REPORT_PREFIX ${file.path} - ok")
                    Mono.empty()
                } else {
                    log.info("$REPORT_PREFIX ${file.path} err: ${status.error}")
                    if (dryRun) {
                        Mono.empty()
                    } else {
                        storage.deleteArchives(listOf(file.path))
                    }
                }
            }.then()
        }
    }

    fun <T> processWith(
        file: ScanningTools.FileChunk,
        reader: AvroReader<T>,
        validator: Validator<T>,
    ): Mono<Status> {
        val values: Flux<T> = storage
            .createReader(file.path)
            .let { input ->
                Flux.from(reader.open(input))
                    .subscribeOn(Schedulers.boundedElastic())
            }

        return values
            .transform(validateWith(validator))
            .doOnError { t -> log.warn("Failed to validate file ${file.path}", t) }
            .next()
    }

    fun <T> validateWith(validator: Validator<T>): Function<Flux<T>, Mono<Status>> {
        val validationMapper: Function<T, Status> = Function { value ->
            val err = validator.validate(value)
            if (err != null) {
                Status.err(err)
            } else {
                Status.ok()
            }
        }

        return Function { values ->
            values.map(validationMapper)
                .doOnError { t -> log.warn("Failed to validate value", t) }
                .filter(Status::isFailed)
                .next()
                .defaultIfEmpty(Status.ok())
        }
    }
}

data class Status(
    val ok: Boolean,
    val error: String? = null,
) {
    companion object {
        @JvmStatic
        fun ok(): Status {
            return Status(true, null)
        }

        @JvmStatic
        fun err(msg: String): Status {
            return Status(false, msg)
        }
    }

    fun isFailed(): Boolean {
        return !ok
    }
}
