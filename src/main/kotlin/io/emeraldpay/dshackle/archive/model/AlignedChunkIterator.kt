package io.emeraldpay.dshackle.archive.model

import kotlin.math.min

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
        if (length == 0L) {
            return emptyList()
        }
        val endBlock = startBlock + length - 1
        val result = ArrayList<Chunk>()
        val fistPosition = startBlock.floorDiv(chunk).times(chunk)
            .let {
                if (it == startBlock) {
                    it
                } else {
                    result.add(Chunk(startBlock, min(it + chunk - startBlock, endBlock - startBlock + 1)))
                    it + chunk
                }
            }

        if (fistPosition >= endBlock) {
            return result
        }

        var currentStart = fistPosition
        while (true) {
            val currentLength = min(chunk, endBlock - currentStart + 1)
            result.add(Chunk(currentStart, currentLength))
            if (currentStart + currentLength - 1 >= endBlock) {
                return result
            }
            currentStart += currentLength
        }
    }
}
