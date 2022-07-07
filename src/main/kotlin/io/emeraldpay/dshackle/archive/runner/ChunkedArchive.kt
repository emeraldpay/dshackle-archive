package io.emeraldpay.dshackle.archive.runner

import io.emeraldpay.dshackle.archive.BlocksRange
import io.emeraldpay.dshackle.archive.model.Chunk
import io.emeraldpay.dshackle.archive.storage.BlockDetails
import io.emeraldpay.dshackle.archive.storage.CompleteWriter
import java.time.Duration
import kotlin.math.roundToInt
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

    fun archiveRanges(blockSource: (chunk: Chunk) -> Flux<BlockDetails>): Mono<Void> {
        return Flux.fromIterable(blocksRange.getChunks()).flatMap({ chunk ->
            val timer = StopWatch.create()
            val data = blockSource(chunk)
            completeWriter.consume(data, chunk)
                    .doOnSubscribe {
                        log.info("Running archive chunk ${chunk.startBlock}..${chunk.endBlock}")
                        timer.start()
                    }
                    .doFinally {
                        val totalTime = Duration.ofMillis(timer.time)
                        if (totalTime.toSeconds() > 0) {
                            val throughput = (chunk.length.toDouble() / totalTime.toSeconds().toDouble() * 60.0 * 10.0).roundToInt().toDouble() / 10.0
                            log.info("Archived in ${totalTime.toMinutes()}m:${StringUtils.leftPad(totalTime.toSecondsPart().toString(), 2, "0")}s at $throughput blocks/min")
                        } else {
                            log.info("Archived in ${totalTime.toMinutes()}m:${StringUtils.leftPad(totalTime.toSecondsPart().toString(), 2, "0")}s:${StringUtils.leftPad(totalTime.toMillisPart().toString(), 2, "0")}ms for ${chunk.length} blocks")
                        }
                    }
        }, 1).then()
    }

}