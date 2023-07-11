package io.emeraldpay.dshackle.archive.model

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
            if (currentLength == 0L) {
                return result
            }
            result.add(Chunk(currentStart, currentLength))
            if (currentStart + currentLength >= endBlock) {
                return result
            }
            currentStart += currentLength
        }
        return result
    }
}
