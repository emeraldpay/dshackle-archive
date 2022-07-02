package io.emeraldpay.dshackle.archive.runner

import io.emeraldpay.api.proto.Common
import io.emeraldpay.api.proto.ReactorBlockchainGrpc
import io.emeraldpay.dshackle.archive.config.RunConfig
import io.emeraldpay.dshackle.archive.storage.CompleteWriter
import io.emeraldpay.dshackle.archive.storage.FilenameGenerator
import io.emeraldpay.dshackle.archive.storage.TargetStorage
import java.time.Duration
import java.time.Instant
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentSkipListSet
import kotlin.collections.HashMap
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import reactor.util.function.Tuples

@Service
@Profile("run-stream")
class RunStream(
        @Autowired private val blockSource: BlockSource,
        @Autowired private val client: ReactorBlockchainGrpc.ReactorBlockchainStub,
        @Autowired private val completeWriter: CompleteWriter,
        @Autowired private val runConfig: RunConfig,
        @Autowired private val targetStorage: TargetStorage,
        @Autowired private val filenameGenerator: FilenameGenerator
) : RunCommand {

    companion object {
        private val log = LoggerFactory.getLogger(RunStream::class.java)
    }

    private val tail = runConfig.range.tail

    override fun run(): Mono<Void> {
        log.info("Initializing stream archival...")
        return blockSource.getCurrentHeight()
                .defaultIfEmpty(0)
                .map { height ->
                    val continueAfter = (height - tail).coerceAtLeast(0)
                    Tuples.of(height, continueAfter)
                }
                .flatMap { processHeight(it.t1, it.t2) }
    }

    fun processHeight(height: Long, continueAfter: Long): Mono<Void> {
        return targetStorage.current.listArchive(listOf(height, continueAfter))
                .flatMap {
                    val range = filenameGenerator.parseRange(it.substringAfterLast("/"))
                            ?: return@flatMap Mono.empty<Long>()
                    Flux.range(range.startBlock.toInt(), range.length.toInt())
                }
                .map { it.toLong() }
                .filter { it in continueAfter until height }
                // a synced height has two files: a block and txes
                // so first count files per height
                .reduce(HashMap<Long, Int>()) { a, b ->
                    a[b] = a.getOrDefault(b, 0) + 1
                    a
                }
                // and now produce only those which have 2 files
                .flatMapMany {
                    Flux.fromIterable(it.entries)
                            .map { Pair(it.key, it.value) }
                            .filter { it.second == 2 }
                            .map { it.first }
                }
                .collectList()
                .defaultIfEmpty(emptyList())
                .map { archivedHeights ->
                    log.info("Start streaming from $height. Ensure blocks from $continueAfter are fully archived")
                    log.debug("Last loaded blocks: ${archivedHeights.size} of $tail")
                    ProcessingWindow(tail, archivedHeights).also {
                        it.onHeight(height)
                    }
                }
                .flatMap(::processWindow)
    }

    fun processWindow(window: ProcessingWindow): Mono<Void> {
        val follow = client.subscribeHead(Common.Chain.newBuilder().setType(Common.ChainRef.forNumber(runConfig.blockchain.id)).build())
                .map { it.height }
                .doOnNext(window::onHeight)

        return Flux.merge(
                follow.subscribeOn(Schedulers.boundedElastic()),
                fillStart(window).subscribeOn(Schedulers.boundedElastic()),
                fillGaps(window).subscribeOn(Schedulers.boundedElastic())
        )
                .filter(window::onStartProcessing)
                .flatMap(blockSource::getDataAtHeight, 4)
                .transform(completeWriter.streamConsumer())
                .doOnNext(window::onProcessed)
                .doOnError { log.error("Stopped processing with unhandled error", it) }
                .then()
    }

    fun fillStart(window: ProcessingWindow): Flux<Long> {
        return window.getMissingHead().let {
            Flux.fromIterable(it)
        }
    }

    fun fillGaps(window: ProcessingWindow): Flux<Long> {
        return Flux.interval(Duration.ofSeconds(60))
                .flatMap {
                    Flux.fromIterable(window.getMissingGaps())
                }
    }

    class ProcessingWindow(
            private val size: Long,
            initial: Collection<Long>
    ) {

        private val processed = ConcurrentSkipListSet<Long>()
        private val processing = ConcurrentHashMap<Long, Instant>()

        init {
            processed.addAll(initial)
        }

        fun onHeight(height: Long) {
            processed.add(height)
        }

        fun onProcessed(height: Long) {
            processing.remove(height)
            processed.add(height)
            if (processed.size > size) {
                processed.removeAll {
                    it < height - size
                }
            }
        }

        fun getMissingHead(): List<Long> {
            val highest = if (processed.isEmpty()) 0 else processed.last()
            val requiredMin = (highest - size).coerceAtLeast(0) //TODO 1 for bitcoin
            val lowest = if (processed.isEmpty()) 0 else processed.first()
            if (lowest > requiredMin) {
                return (requiredMin until lowest).toList()
            }
            return emptyList()
        }

        fun getMissingGaps(): List<Long> {
            val result = LinkedList<Long>()
            var prev = processed.first()
            processed.drop(1).forEach {
                val diff = it - prev
                if (diff > 1) {
                    result.addAll(((prev + 1) until it))
                }
                prev = it
            }
            return result
        }

        fun failed(height: Long) {
            processing.remove(height)
        }

        fun isProcessing(height: Long): Boolean {
            return processing[height]?.let { it > Instant.now() - Duration.ofMinutes(1) } ?: false
        }

        fun isNotProcessing(height: Long): Boolean {
            return !isProcessing(height)
        }

        fun onStartProcessing(height: Long): Boolean {
            if (isProcessing(height)) {
                return false
            }
            processing[height] = Instant.now()
            if (processing.size > size) {
                processing.forEach { (a, b) ->
                    if (b < Instant.now() - Duration.ofMinutes(1)) {
                        processing.remove(a)
                    }
                }
            }
            return true
        }
    }

}