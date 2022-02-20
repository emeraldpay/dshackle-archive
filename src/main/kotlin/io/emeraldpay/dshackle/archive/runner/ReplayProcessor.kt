package io.emeraldpay.dshackle.archive.runner

import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.JsonToken
import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream
import io.emeraldpay.etherjar.domain.TransactionId
import java.nio.ByteBuffer
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono

class ReplayProcessor {

    companion object {
        private val log = LoggerFactory.getLogger(ReplayProcessor::class.java)
    }

    private val jsonFactory = JsonFactory()

    fun start(resp: Mono<ByteBuffer>): ReplyValues {
        return ReplyValuesImpl(
                resp
                        .map { splitReplayList(it) }
                        .doOnError { t -> log.warn("Failed to process replayTransactions", t) }
        )
    }

    fun startAsync(resp: Mono<ByteBuffer>): Mono<out ReplyValues> {
        return resp
                .map { splitReplayList(it) }
                .map { ReplyValuesProvided(it) }
                .doOnError { t -> log.warn("Failed to process replayTransactions", t) }
    }

    fun empty(): ReplyValues {
        return ReplyValueEmpty()
    }

    fun splitReplayList(json: ByteBuffer): List<TxTrace> {
        val parser: JsonParser = jsonFactory.createParser(ByteBufferBackedInputStream(json))
        parser.nextToken()
        val results = ArrayList<TxTrace>()
        while (parser.nextToken() == JsonToken.START_OBJECT) {
            results.add(extractTransaction(parser, json))
        }
        return results
    }

    fun extractTransaction(parser: JsonParser, json: ByteBuffer): TxTrace {
        val start = parser.tokenLocation
        var txid: TransactionId? = null
        while (parser.nextToken() != JsonToken.END_OBJECT) {
            if (parser.nextToken() == JsonToken.START_OBJECT) {
                parser.skipChildren()
            } else {
                val field = parser.currentName
                if (txid == null && field == "transactionHash") {
                    parser.nextToken()
                    txid = TransactionId.from(parser.valueAsString)
                }
            }
        }
        if (txid == null) {
            throw IllegalStateException("No transaction id found")
        }
        val length = parser.currentLocation.byteOffset - start.byteOffset
        val copy = json.slice(start.byteOffset.toInt(), length.toInt())
        return TxTrace(txid, copy)
    }

    data class TxTrace(val txid: TransactionId, val json: ByteBuffer)

    interface ReplyValues {
        fun get(txid: TransactionId): Mono<TxTrace>
    }

    class ReplyValueEmpty : ReplyValues {
        override fun get(txid: TransactionId): Mono<TxTrace> {
            return Mono.empty()
        }
    }

    class ReplyValuesImpl(
            source: Mono<List<TxTrace>>
    ) : ReplyValues {

        private val shared = source.share()

        override fun get(txid: TransactionId): Mono<TxTrace> {
            return Mono.from(shared).flatMap {
                val value = it.find { it.txid == txid }
                if (value == null) {
                    log.warn("No value for $txid")
                }
                Mono.justOrEmpty(value)
            }
        }
    }

    class ReplyValuesProvided(
            private val source: List<TxTrace>
    ) : ReplyValues {

        override fun get(txid: TransactionId): Mono<TxTrace> {
            val value = source.find { it.txid == txid }
            if (value == null) {
                log.warn("No value for $txid")
            }
            return Mono.justOrEmpty(value)
        }
    }

}