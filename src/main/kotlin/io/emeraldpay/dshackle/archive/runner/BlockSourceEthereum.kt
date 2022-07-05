package io.emeraldpay.dshackle.archive.runner

import com.fasterxml.jackson.databind.ObjectMapper
import io.emeraldpay.api.proto.ReactorBlockchainGrpc
import io.emeraldpay.dshackle.archive.config.RunConfig
import io.emeraldpay.dshackle.archive.storage.BlockDetails
import io.emeraldpay.dshackle.archive.storage.TransactionDetails
import io.emeraldpay.etherjar.domain.TransactionId
import io.emeraldpay.etherjar.hex.HexData
import io.emeraldpay.etherjar.hex.HexQuantity
import io.emeraldpay.etherjar.rpc.json.BlockJson
import io.emeraldpay.etherjar.rpc.json.TransactionJson
import io.emeraldpay.etherjar.rpc.json.TransactionReceiptJson
import io.emeraldpay.etherjar.rpc.json.TransactionRefJson
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.util.function.Tuples
import java.nio.ByteBuffer
import java.util.*

@Service
@Profile("!run-copy & !run-compact & !run-report & ethereum")
class BlockSourceEthereum(
        @Autowired private val client: ReactorBlockchainGrpc.ReactorBlockchainStub,
        @Autowired private val objectMapper: ObjectMapper,
        @Autowired private val runConfig: RunConfig
) : BlockSource(runConfig, client, objectMapper) {

    companion object {
        private val log = LoggerFactory.getLogger(BlockSourceEthereum::class.java)

    }

    private val replayProcessor = ReplayProcessor()

    init {
        log.info("Use blockchain: $chain")
    }

    override fun getDataAtHeight(height: Long): Mono<BlockDetails> {
        return getBlock(height)
                .flatMap { block ->
                    getUncles(block.result!!)
                            .map { it.map { it.raw } }
                            .map { Tuples.of(block.raw, block.result, it) }
                            .doOnError { t ->
                                log.error("Failed to get uncles for ${block.result.number} ${block.result.hash}", t)
                            }
                }
                .flatMap {
                    val block = it.t2
                    val uncles = it.t3
                    getTransactions(it.t2)
                            .map { txes ->
                                BlockDetails(
                                        timestamp = block.timestamp,
                                        hash = block.hash.toHex(),
                                        parentHash = block.parentHash.toHex(),
                                        height = block.number,
                                        transactionHashes = block.transactions.map { it.hash.toHex() },
                                        raw = it.t1,
                                        transactions = txes,
                                        uncles = uncles)
                            }
                            .doOnError { t ->
                                log.error("Failed to get txes for ${it.t2.number} ${it.t2.hash}", t)
                            }
                }
    }

    override fun getCurrentHeight(): Mono<Long> {
        return executeAndRead("eth_blockNumber", emptyList(), String::class.java).map {
            HexQuantity.from(it.result!!).value.longValueExact()
        }
    }

    fun getBlock(height: Long): Mono<Result<BlockJson<TransactionRefJson>>> {
        return executeAndRead<BlockJson<*>>(
                "eth_getBlockByNumber", listOf(HexQuantity.from(height).toHex(), false),
                BlockJson::class.java
        ) as Mono<Result<BlockJson<TransactionRefJson>>>
    }

    fun getUncles(block: BlockJson<*>): Mono<List<Result<BlockJson<TransactionRefJson>>>> {
        if (block.uncles == null || block.uncles.isEmpty()) {
            return Mono.just(emptyList())
        }
        return Flux.range(0, block.uncles.size)
                .flatMap {
                    executeAndRead(
                            "eth_getUncleByBlockHashAndIndex", listOf(block.hash.toHex(), HexQuantity.from(it.toLong()).toHex()),
                            BlockJson::class.java
                    ) as Mono<Result<BlockJson<TransactionRefJson>>>
                }
                .collectList()
    }

    fun getTransaction(txid: TransactionId, stateDiffSource: ReplayProcessor.ReplyValues): Mono<TransactionDetails> {
        val json = executeAndRead("eth_getTransactionByHash", listOf(txid.toHex()), TransactionJson::class.java).doOnNext {
            if (it.result == null) {
                //val json = String(ByteBufferBackedInputStream(it.raw).readAllBytes())
                log.warn("No transaction for $txid")
            }
        }.doOnError { t ->
            log.error("Failed to read Transaction Details $txid. ${t.message}")
        }
        val receiptJson = executeAndRead("eth_getTransactionReceipt", listOf(txid.toHex()), TransactionReceiptJson::class.java).doOnError { t ->
            log.error("Failed to read Transaction Receipt $txid. ${t.message}")
        }
        val raw = executeAndRead("eth_getRawTransactionByHash", listOf(txid.toHex()), String::class.java).map {
            // reading as a hex string, but storing as actual bytes
            ByteBuffer.wrap(HexData.from(it.result).bytes)
        }.doOnError { t ->
            log.error("Failed to read Raw Transaction $txid. ${t.message}")
        }

        val traceJson: Mono<Optional<ByteBuffer>> = if (runConfig.options.trace) {
            execute("debug_traceTransaction", listOf(txid.toHex())).map {
                Optional.of(it)
            }.doOnError { t ->
                log.error("Failed to Trace Transaction $txid. ${t.message}")
            }
        } else {
            Mono.just(Optional.empty())
        }

        val stateDiffJson: Mono<Optional<ByteBuffer>> = if (runConfig.options.stateDiff) {
            // for some unknown reason Erigon may return null for some blocks when you request a whole block,
            // in this case try to read the tx trace directly
            val readDirectly = execute("trace_replayTransaction", listOf(txid.toHex(), listOf("stateDiff")))
                    .map { ReplayProcessor.TxTrace(txid, it) }
                    .doOnNext { log.info("Tx trace recovered for $txid") }
            stateDiffSource.get(txid)
                    .switchIfEmpty(readDirectly)
                    .map { Optional.of(it.json) }
                    .defaultIfEmpty(Optional.empty())
                    .doOnError { t ->
                        log.error("Failed to read Transaction State Diff $txid. ${t.message}")
                    }
        } else {
            Mono.just(Optional.empty())
        }

        return Mono.zip(json, receiptJson, raw, traceJson, stateDiffJson).map {
            val jsonResult = it.t1
            val receiptResult = it.t2
            TransactionDetails(
                    hash = jsonResult.result!!.hash.toHex(),
                    raw = it.t3,
                    json = jsonResult.raw,
                    from = jsonResult.result.from.toHex(),
                    to = jsonResult.result.to?.toHex(),
                    receiptJson = receiptResult.raw,
                    traceJson = it.t4.orElse(null),
                    stateDiff = it.t5.orElse(null)
            )
        }
    }

    fun getTransactions(block: BlockJson<TransactionRefJson>): Mono<List<TransactionDetails>> {
        // requesting state changes for the whole block is about 10x faster than requesting all of them per individual transaction
        val blockState = if (runConfig.options.stateDiff) {
            replayProcessor.start(
                    execute("trace_replayBlockTransactions", listOf(block.hash.toHex(), listOf("stateDiff")))
            )
        } else {
            replayProcessor.empty()
        }
        return Flux.fromIterable(block.transactions)
                .flatMapSequential({ getTransaction(it.hash, blockState) }, parallelTx)
                .collectList()
    }


}