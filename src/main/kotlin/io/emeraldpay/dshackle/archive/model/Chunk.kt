package io.emeraldpay.dshackle.archive.model

data class Chunk(val startBlock: Long, val length: Long) {

    init {
        check(length >= 0) {
            "Chunk Length cannot be a negative number"
        }
        check(startBlock >= 0) {
            "Chunk Start Block cannot be a negative number"
        }
    }

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

    fun findContinuity(all: Iterable<Chunk>): Chunk? {
        return all.find {
            it.intersects(this) || it.endBlock + 1 == this.startBlock || this.endBlock + 1 == it.startBlock
        }
    }

    fun join(another: Chunk): Chunk {
        val startBlock = this.startBlock.coerceAtMost(another.startBlock)
        val endBlock = this.endBlock.coerceAtLeast(another.endBlock)
        val length = endBlock - startBlock + 1
        return Chunk(startBlock, length)
    }

    fun mergeContinuing(existing: Iterable<Chunk>): List<Chunk> {
        return findContinuity(existing)?.let { neighbor ->
            val currentJoined = neighbor.join(this)
            val others = existing.filterNot { it == neighbor }
            currentJoined.mergeContinuing(others)
        } ?: (listOf(this) + existing)
    }
}