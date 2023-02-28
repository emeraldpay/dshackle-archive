package io.emeraldpay.dshackle.archive.runner

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream
import com.google.protobuf.ByteString
import io.emeraldpay.api.blockchain.BlockchainApi
import io.emeraldpay.api.proto.BlockchainOuterClass
import io.emeraldpay.api.proto.Common
import io.emeraldpay.dshackle.archive.config.RunConfig
import io.emeraldpay.dshackle.archive.storage.BlockDetails
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import reactor.util.retry.Retry
import java.net.ConnectException
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicInteger

abstract class BlockSource(
    private val runConfig: RunConfig,
    private val client: BlockchainApi,
    private val objectMapper: ObjectMapper
) {

    companion object {
        private val log = LoggerFactory.getLogger(BlockSource::class.java)
    }

    private val timeout = runConfig.connection!!.timeout
    val parallel = runConfig.connection!!.parallel
            .coerceAtLeast(4)
    val parallelBlock = parallel
            .coerceAtLeast(1)
            .coerceAtMost(4)
    val parallelTx = (parallelBlock * 8)
            .coerceAtMost(parallel)

    protected val scheduler = Schedulers.newBoundedElastic(
            parallel, Integer.MAX_VALUE, "blockchain-data"
    )
    protected val chain = Common.ChainRef.forNumber(runConfig.blockchain.id)
    protected val id = AtomicInteger(0)

    init {
        log.info("Run up to $parallel requests in parallel (query $parallelBlock blocks * $parallelTx tx)")
    }

    abstract fun getDataAtHeight(height: Long): Mono<BlockDetails>
    abstract fun getBlockIdAtHeight(height: Long): Mono<String>
    abstract fun getCurrentHeight(): Mono<Long>

    fun getData(start: Long, limit: Long): Flux<BlockDetails> {
        return Flux.range(start.toInt(), limit.toInt())
                .flatMapSequential({
                    getDataAtHeight(it.toLong()).subscribeOn(scheduler)
                }, parallelBlock, parallelBlock)
                .onBackpressureBuffer(64) {
                    log.error("Too many blocks in queue to archive. Decrease the value for --parallel option")
                }
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
                    client.reactorStub.nativeCall(call)
                            .subscribeOn(scheduler)
                            .timeout(
                                    timeout,
                                    Mono.fromCallable { retried = true }.then(Mono.error(TimeoutException("Timeout to load $method($params)")))
                            )
                            .doOnError { t ->
                                if (t is TimeoutException) {
                                    log.warn(t.message)
                                } else if (t is ConnectException || t is io.grpc.StatusRuntimeException) {
                                    log.error("Cannot connect to the Blockchain ${runConfig.connection?.describe()}: ${t.message}")
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
