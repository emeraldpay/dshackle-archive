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

    var length: Long = range.count
    var startBlock: Long = range.start
    val endBlock: Long = range.start + length - 1
    val isUsingRanges: Boolean = !range.individual

    fun getChunks(): List<Chunk> {
        // note that the startBlock may change after startup, because archive can rewind to after the latest archive block,
        // so we cannot initialize the iterator as a field
        val chunkIterator: ChunkIterator = if (range.positioned) {
            AlignedChunkIterator(startBlock, length, range.chunk)
        } else {
            StandardChunkIterator(startBlock, length, range.chunk)
        }
        return chunkIterator.getChunks()
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

        val endBlock: Long
            get() {
                return startBlock + length - 1
            }

        fun intersects(o: Chunk): Boolean {
            val oEnd = o.endBlock
            val end = endBlock

            val crossLeft = startBlock in o.startBlock..oEnd
            val crossRight = end in o.startBlock..oEnd
            val includes = startBlock <= o.startBlock && end >= oEnd
            val inside = startBlock >= o.startBlock && end <= oEnd

            return crossLeft || crossRight || includes || inside
        }

        fun findContinuity(all: List<Chunk>): Chunk? {
            return all.find {
                it.intersects(this) || it.endBlock + 1 == this.startBlock || this.endBlock + 1 == it.startBlock
            }
        }

        fun join(another: Chunk): Chunk {
            val startBlock = this.startBlock.coerceAtMost(another.startBlock)
            val endBlock = this.endBlock.coerceAtLeast(another.endBlock)
            val length = endBlock - startBlock
            return Chunk(startBlock, length)
        }

        fun mergeContinuing(existing: List<Chunk>): List<Chunk> {
            return findContinuity(existing)?.let { neighbor ->
                existing.filterNot { it == neighbor } + listOf(neighbor.join(this))
            } ?: (listOf(this) + existing)
        }
    }

    interface ChunkIterator {
        fun getChunks(): List<Chunk>
    }

    class StandardChunkIterator(
            val startBlock: Long,
            val length: Long,
            val chunk: Long,
    ) : ChunkIterator {

        override fun getChunks(): List<Chunk> {
            val endBlock = startBlock + length
            val result = ArrayList<Chunk>()
            var currentStart = startBlock
            while (true) {
                val currentLength = chunk.coerceAtMost(endBlock - currentStart)
                result.add(Chunk(currentStart, currentLength))
                if (currentStart + currentLength >= endBlock) {
                    return result
                }
                currentStart += currentLength
            }
            return result
        }
    }

    /**
     * Chunks are aligned by chunk size. I.e. if chunk size is 100 and range started at 80 then the first chunk
     * is 80..99, then next is 100..200
     */
    class AlignedChunkIterator(
            val startBlock: Long,
            val length: Long,
            val chunk: Long,
    ) : ChunkIterator {

        override fun getChunks(): List<Chunk> {
            val result = ArrayList<Chunk>()
            val fistPosition = startBlock.floorDiv(chunk).times(chunk).let {
                if (it == startBlock) {
                    it
                } else {
                    result.add(Chunk(startBlock, it + chunk - startBlock))
                    it + chunk
                }
            }

            val endBlock = startBlock + length
            var currentStart = fistPosition
            while (true) {
                val currentLength = chunk.coerceAtMost(endBlock - currentStart)
                result.add(Chunk(currentStart, currentLength))
                if (currentStart + currentLength >= endBlock) {
                    return result
                }
                currentStart += currentLength
            }
            return result
        }
    }
}