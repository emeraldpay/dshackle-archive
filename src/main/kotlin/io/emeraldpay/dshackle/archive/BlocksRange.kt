package io.emeraldpay.dshackle.archive

import io.emeraldpay.dshackle.archive.config.RunConfig
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service

@Service
class ConfiguredBlocksRange(
        @Autowired private val runConfig: RunConfig
) : BlocksRange(runConfig.range)

open class BlocksRange(
        val range: RunConfig.Range
) {

    companion object {
        private val log = LoggerFactory.getLogger(BlocksRange::class.java)
    }

    val length: Long = range.count
    var startBlock: Long = range.start
    val endBlock: Long = range.start + length - 1
    val isUsingRanges: Boolean = !range.individual

    fun getChunks(): List<Chunk> {
        val endBlock = startBlock + length
        val result = ArrayList<Chunk>()
        var currentStart = startBlock
        while (true) {
            val currentLength = range.chunk
                    .coerceAtMost(endBlock - currentStart)
            result.add(Chunk(currentStart, currentLength))
            if (currentStart + currentLength >= endBlock) {
                return result
            }
            currentStart += currentLength
        }
        return result
    }

    fun getChunkAt(height: Long): Chunk {
        if (height < startBlock) {
            throw IllegalStateException("Cannot get chunk at $height. Range starts from $startBlock")
        }
        val startBlock = height.floorDiv(range.chunk) * range.chunk
        return Chunk(startBlock, range.chunk)
    }

    fun wholeChunk(): Chunk {
        return Chunk(startBlock, length)
    }

    fun includes(height: Long): Boolean {
        return height in startBlock..endBlock
    }

    data class Chunk(val startBlock: Long, val length: Long) {

        fun endBlock(): Long {
            return startBlock + length - 1
        }

        fun intersects(o: Chunk): Boolean {
            val oEnd = o.endBlock()
            val end = endBlock()

            val crossLeft = startBlock in o.startBlock..oEnd
            val crossRight = end in o.startBlock..oEnd
            val includes = startBlock <= o.startBlock && end >= oEnd
            val inside = startBlock >= o.startBlock && end <= oEnd

            return crossLeft || crossRight || includes || inside
        }

    }
}