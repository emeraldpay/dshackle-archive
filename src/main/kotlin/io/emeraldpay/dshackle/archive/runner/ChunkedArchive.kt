package io.emeraldpay.dshackle.archive.runner

import io.emeraldpay.dshackle.archive.BlocksRange
import io.emeraldpay.dshackle.archive.storage.BlockDetails
import io.emeraldpay.dshackle.archive.storage.CompleteWriter
import java.time.Duration
import java.time.Instant
import java.util.concurrent.TimeUnit
import kotlin.math.roundToInt
import kotlin.time.toDuration
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.time.StopWatch
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

class ChunkedArchive(
        val blocksRange: BlocksRange,
        val completeWriter: CompleteWriter
) {

    companion object {
        private val log = LoggerFactory.getLogger(ChunkedArchive::class.java)
    }

    fun archiveRanges(blockSource: (chunk: BlocksRange.Chunk) -> Flux<BlockDetails>): Mono<Void> {
        return Flux.fromIterable(blocksRange.getChunks()).flatMap({ chunk ->
            val timer = StopWatch.create()
            val data = blockSource(chunk)
            completeWriter.consume(data, chunk)
                    .doOnSubscribe {
                        log.info("Running archive chunk ${chunk.startBlock}..${chunk.startBlock + chunk.length - 1}")
                        timer.start()
                    }
                    .doFinally {
                        val totalTime = Duration.ofMillis(timer.time)
                        val throughput = (chunk.length.toDouble() / totalTime.toSeconds().toDouble() * 60.0 * 10.0).roundToInt().toDouble() / 10.0
                        log.info("Archived in ${totalTime.toMinutes()}m:${StringUtils.leftPad(totalTime.toSecondsPart().toString(), 2, "0")}s at $throughput blocks/min")
                    }
        }, 1).then()
    }

}