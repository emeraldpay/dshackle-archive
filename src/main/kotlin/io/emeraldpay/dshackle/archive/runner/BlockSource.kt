package io.emeraldpay.dshackle.archive.runner

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream
import com.google.protobuf.ByteString
import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.Common
import io.emeraldpay.api.proto.ReactorBlockchainGrpc
import io.emeraldpay.dshackle.archive.config.RunConfig
import io.emeraldpay.dshackle.archive.storage.BlockDetails
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicInteger
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import reactor.util.retry.Retry

abstract class BlockSource(
        runConfig: RunConfig,
        private val client: ReactorBlockchainGrpc.ReactorBlockchainStub,
        private val objectMapper: ObjectMapper
) {

    companion object {
        private val log = LoggerFactory.getLogger(BlockSource::class.java)

        private const val threads = 20
    }

    private val timeout = runConfig.connection!!.timeout
    val parallelBlock = runConfig.connection!!.parallel
    val parallelTx = parallelBlock * 4

    protected val scheduler = Schedulers.boundedElastic()
    protected val chain = Common.ChainRef.forNumber(runConfig.blockchain.id)
    protected val id = AtomicInteger(0)

    abstract fun getDataAtHeight(height: Long): Mono<BlockDetails>
    abstract fun getCurrentHeight(): Mono<Long>

    fun getData(start: Long, limit: Long): Flux<BlockDetails> {
        return Flux.range(start.toInt(), limit.toInt())
                .flatMapSequential({
                    getDataAtHeight(it.toLong()).subscribeOn(scheduler)
                }, parallelBlock)
    }

    fun executeAndReadMap(method: String, params: List<Any>): Mono<Result<Map<String, Any>>> {
        return executeAndRead(method, params, Map::class.java).map {
            it as Result<Map<String, Any>>
        }
    }

    fun <T> executeAndRead(method: String, params: List<Any>, target: Class<T>): Mono<Result<T>> {
        return execute(method, params).map { buffer ->
            buffer.mark()
            val result = try {
                objectMapper.readerFor(target)
                        .readValue<T>(ByteBufferBackedInputStream(buffer))
            } catch (t: Throwable) {
                buffer.reset()
                val json = StringUtils.abbreviateMiddle(
                        StandardCharsets.UTF_8.decode(buffer).toString(), "...", 250
                )
                log.warn("Cannot parse JSON. Call: $method($params). Error: ${t.javaClass} ${t.message} Target class: $target. JSON: $json")
                throw t
            }
            buffer.reset()
            Result(buffer, result)
        }
    }

    fun execute(method: String, params: List<Any>): Mono<ByteBuffer> {
        val callItem = BlockchainOuterClass.NativeCallItem.newBuilder()
                .setId(id.incrementAndGet())
                .setMethod(method)
                .setPayload(ByteString.copyFromUtf8(objectMapper.writeValueAsString(params)))
                .build()
        var retried = false
        return Mono.just(callItem)
                .flatMapMany {
                    val call = BlockchainOuterClass.NativeCallRequest.newBuilder()
                            .setChain(chain)
                            .addItems(callItem)
                            .build()
                    client.nativeCall(call)
                            .subscribeOn(scheduler)
                            .timeout(
                                    timeout,
                                    Mono.fromCallable { retried = true }.then(Mono.error(TimeoutException("Timeout to load $method($params)")))
                            )
                            .doOnError { t ->
                                if (t is TimeoutException) {
                                    log.warn(t.message)
                                } else {
                                    log.error("Unhandled error for $method($params)", t)
                                }
                            }
                            // fast retry for connection errors
                            .retryWhen(Retry.backoff(3, Duration.ofSeconds(1)))
                            .map { reply ->
                                if (!reply.succeed) {
                                    throw IllegalStateException("Not succeeded: ${reply.errorMessage}")
                                }
                                if (retried) {
                                    log.info("Recovered for $method($params)")
                                }
                                reply.payload.asReadOnlyByteBuffer()
                            }
                }
                // a slow retry when operation did not succeed on remote side, ex. when dshackle lost its upstreams
                .retryWhen(Retry.backoff(25, Duration.ofSeconds(5)))
                .doOnError { t ->
                    log.error("Failed to get $method", t)
                }
                .single()
    }

    data class Result<T>(
            val raw: ByteBuffer,
            val result: T?
    )
}