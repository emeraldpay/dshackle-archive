package io.emeraldpay.dshackle.archive.runner

import io.emeraldpay.dshackle.archive.BlocksRange
import io.emeraldpay.dshackle.archive.storage.BlockDetails
import io.emeraldpay.dshackle.archive.storage.CompleteWriter
import java.time.Duration
import java.time.Instant
import kotlin.math.roundToInt
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux

class ChunkedArchive(
        val blocksRange: BlocksRange,
        val completeWriter: CompleteWriter
) {

    companion object {
        private val log = LoggerFactory.getLogger(ChunkedArchive::class.java)
    }

    fun archiveRanges(blockSource: (chunk: BlocksRange.Chunk) -> Flux<BlockDetails>) {
        blocksRange.getChunks().forEach { chunk ->
            log.info("Running archive chunk ${chunk.startBlock}..${chunk.startBlock + chunk.length - 1}")
            val timeStart = Instant.now()
            val data = blockSource(chunk)
            completeWriter.consume(data, chunk)
            val totalTime = Duration.between(timeStart, Instant.now())
            val throughput = (chunk.length.toDouble() / totalTime.toSeconds().toDouble() * 60.0 * 10.0).roundToInt().toDouble() / 10.0
            log.info("Archived in ${totalTime.toMinutes()}m:${StringUtils.leftPad(totalTime.toSecondsPart().toString(), 2, "0")}s at $throughput blocks/min")
        }
    }

}