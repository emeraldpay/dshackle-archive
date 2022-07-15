package io.emeraldpay.dshackle.archive.model

data class Chunk(val startBlock: Long, val length: Long) {

    companion object {
        fun between(startBlock: Long, endBlock: Long): Chunk {
            return Chunk(startBlock, endBlock - startBlock + 1)
        }
    }

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
    val isEmpty: Boolean
        get() {
            return length == 0L
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

    fun includes(o: Chunk): Boolean {
        val oEnd = o.endBlock
        val end = endBlock

        return startBlock <= o.startBlock && end >= oEnd
    }

    fun findContinuity(all: Iterable<Chunk>): Chunk? {
        return all.find {
            it.intersects(this) || it.endBlock + 1 == this.startBlock || this.endBlock + 1 == it.startBlock
        }
    }

    fun join(another: Chunk): Chunk {
        val startBlock = this.startBlock.coerceAtMost(another.startBlock)
        val endBlock = this.endBlock.coerceAtLeast(another.endBlock)
        return between(startBlock, endBlock)
    }

    fun cut(another: Chunk): Chunk {
        if (another == this || this.isEmpty || another.isEmpty) {
            return Chunk(startBlock, 0)
        }
        if (another.includes(this)) {
            return Chunk(another.startBlock, 0)
        }
        val left = another.startBlock <= this.startBlock && another.endBlock >= this.startBlock
        if (left) {
            return between(another.endBlock + 1, this.endBlock)
        }
        val right = another.endBlock >= this.endBlock && another.startBlock >= this.startBlock && another.startBlock <= this.endBlock
        if (right) {
            return between(this.startBlock, another.startBlock - 1)
        }
        return this
    }

    fun splitBy(another: Chunk): Pair<Chunk, Chunk> {
        require(this.includes(another)) { "Cannot split by a chunk not inside the current" }
        val left = between(this.startBlock, another.startBlock - 1)
        val right = between(another.endBlock + 1, this.endBlock)
        return Pair(left, right)
    }

    fun mergeContinuing(existing: Iterable<Chunk>): List<Chunk> {
        return findContinuity(existing)?.let { neighbor ->
            val currentJoined = neighbor.join(this)
            val others = existing.filterNot { it == neighbor }
            currentJoined.mergeContinuing(others)
        } ?: (listOf(this) + existing)
    }
}