package io.emeraldpay.dshackle.archive

import io.emeraldpay.dshackle.archive.config.RunConfig
import io.emeraldpay.dshackle.archive.model.AlignedChunkIterator
import io.emeraldpay.dshackle.archive.model.Chunk
import io.emeraldpay.dshackle.archive.model.ChunkIterator
import io.emeraldpay.dshackle.archive.model.StandardChunkIterator
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service

@Service
class ConfiguredBlocksRange(
    @Autowired private val runConfig: RunConfig,
) : BlocksRange(runConfig.range)

open class BlocksRange(
    val range: RunConfig.Range,
) {

    companion object {
        private val log = LoggerFactory.getLogger(BlocksRange::class.java)
    }

    var length: Long = range.count
    var startBlock: Long = range.start
    val endBlock: Long
        get() = startBlock + length - 1
    val isUsingRanges: Boolean = !range.individual

    init {
        check(length >= 0) {
            "Blocks Range Length cannot be negative number"
        }
        check(startBlock >= 0) {
            "Blocks Range Start Block cannot be negative number"
        }
    }

    fun getChunks(): List<Chunk> {
        // note that the startBlock may change after startup, because archive can rewind to after the latest archive block,
        // so we cannot initialize the iterator as a field
        val chunkIterator: ChunkIterator = if (range.aligned) {
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
}
