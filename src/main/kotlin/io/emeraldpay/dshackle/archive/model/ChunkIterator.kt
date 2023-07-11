package io.emeraldpay.dshackle.archive.model

interface ChunkIterator {
    fun getChunks(): List<Chunk>
}
