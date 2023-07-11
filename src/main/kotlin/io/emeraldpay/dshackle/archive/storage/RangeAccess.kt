package io.emeraldpay.dshackle.archive.storage

import io.emeraldpay.dshackle.archive.BlocksRange
import io.emeraldpay.dshackle.archive.config.RunConfig
import org.slf4j.LoggerFactory

class RangeAccess(
    private val runConfig: RunConfig,
) {

    companion object {
        private val log = LoggerFactory.getLogger(RangeAccess::class.java)
    }

    fun findHeightsToCheck(blocksRange: BlocksRange): List<Long> {
        return findHeightsToCheck(blocksRange.startBlock, blocksRange.endBlock)
    }

    fun findHeightsToCheck(startBlock: Long, endBlock: Long): List<Long> {
        val heights = mutableSetOf<Long>()
        heights.add(startBlock)
        var height = startBlock
        while (height < endBlock) {
            height += runConfig.range.chunk
            heights.add(height)
        }
        return heights.toList()
    }
}
