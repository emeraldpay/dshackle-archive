package io.emeraldpay.dshackle.archive.runner

import com.fasterxml.jackson.databind.ObjectMapper
import io.emeraldpay.api.blockchain.BlockchainApi
import io.emeraldpay.dshackle.archive.config.RunConfig
import io.emeraldpay.dshackle.archive.storage.BlockDetails
import io.emeraldpay.dshackle.archive.storage.TransactionDetails
import org.apache.commons.codec.binary.Hex
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.nio.ByteBuffer
import java.time.Instant

@Service
@Profile("!run-copy & !run-compact & !run-report & bitcoin & with-blockchain")
class BlockSourceBitcoin(
    @Autowired private val client: BlockchainApi,
    @Autowired private val objectMapper: ObjectMapper,
    @Autowired private val runConfig: RunConfig,
) : BlockSource(runConfig, client, objectMapper) {

    companion object {
        private val log = LoggerFactory.getLogger(BlockSourceBitcoin::class.java)
    }

    override fun getBlockIdAtHeight(height: Long): Mono<String> {
        return executeAndRead("getblockhash", listOf(height), String::class.java).map {
            it.result ?: ""
        }
    }

    override fun getDataAtHeight(height: Long): Mono<BlockDetails> {
        return executeAndRead("getblockhash", listOf(height), String::class.java).flatMap { hash ->
            executeAndReadMap("getblock", listOf(hash.result!!)).flatMap { block ->
                getTransactions(block.result!!).map { txes ->
                    toDetails(block.result, block.raw, txes)
                }
            }
        }
    }

    override fun getCurrentHeight(): Mono<Long> {
        return executeAndRead("getbestblockhash", emptyList(), String::class.java).flatMap { hash ->
            executeAndReadMap("getblock", listOf(hash.result!!)).map { block ->
                (block.result!!["height"] as Number).toLong()
            }
        }
    }

    fun getTransactions(block: Map<String, Any>): Mono<List<TransactionDetails>> {
        val txes: List<String> = block["tx"] as List<String>
        return Flux.fromIterable(txes)
            .flatMap(::getTransaction, parallelTx)
            .collectList()
    }

    fun getTransaction(hash: String): Mono<TransactionDetails> {
        return executeAndReadMap("getrawtransaction", listOf(hash, true)).map { tx ->
            TransactionDetails(
                hash = tx.result!!["txid"] as String,
                raw = ByteBuffer.wrap(Hex.decodeHex(tx.result["hex"] as String)),
                json = tx.raw,
            )
        }
    }

    fun toDetails(block: Map<String, Any>, blockRaw: ByteBuffer, transactions: List<TransactionDetails>): BlockDetails {
        return BlockDetails(
            timestamp = Instant.ofEpochSecond((block["time"] as Number).toLong()),
            height = (block["height"] as Number).toLong(),
            hash = block["hash"] as String,
            parentHash = block["previousblockhash"] as String,
            raw = blockRaw,
            transactionHashes = block["tx"] as List<String>,
            transactions = transactions,
        )
    }
}
