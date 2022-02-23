package io.emeraldpay.dshackle.archive.storage

import io.emeraldpay.dshackle.archive.BlocksRange
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory

open class FilenameGenerator(
        val versionId: String,
        val parentDir: String,
        private val dirBlockSizeL1: Long = 1_000_000,
        private val dirBlockSizeL2: Long = 1_000,
) {

    companion object {
        private val log = LoggerFactory.getLogger(FilenameGenerator::class.java)
    }

    private val rangeRegex = Regex("range-(\\d+)_(\\d+)\\.(\\w+)\\.(\\w+\\.)?avro")
    private val singleRegex = Regex("(\\d+)\\.(\\w+)\\.(\\w+\\.)?avro")
    private val version = if (StringUtils.isNotEmpty(versionId)) {
        ".$versionId"
    } else ""

    init {
        check(dirBlockSizeL1 > dirBlockSizeL2)
        check(dirBlockSizeL1 > 1)
        check(StringUtils.isEmpty(versionId) || StringUtils.isAlphanumeric(versionId)) {
            "Version should be ALPHA-NUMERIC. Value: $versionId"
        }
    }

    fun parseRange(filename: String): BlocksRange.Chunk? {
        val range = rangeRegex.matchEntire(filename.substringAfterLast("/"))
        if (range != null) {
            val start = range.groupValues[1].toLong()
            val end = range.groupValues[2].toLong()
            return BlocksRange.Chunk(start, end - start + 1)
        }
        val single = singleRegex.matchEntire(filename.substringAfterLast("/"))
        if (single != null) {
            val height = single.groupValues[1].toLong()
            return BlocksRange.Chunk(height, 1)
        }
        return null
    }

    fun getRangeFilename(type: String, chunk: BlocksRange.Chunk): String {
        val level0 = chunk.startBlock / dirBlockSizeL1 * dirBlockSizeL1
        return listOf(
                parentDir,
                rangePadded(level0), "/",
                "range-",
                rangePadded(chunk.startBlock), "_", rangePadded(chunk.startBlock + chunk.length - 1),
                ".",
                type,
                version,
                ".avro"
        ).joinToString("")
    }

    fun getLevel0(height: Long): String {
        val level0 = height / dirBlockSizeL1 * dirBlockSizeL1
        return rangePadded(level0)
    }

    fun getIndividualFilename(type: String, height: Long): String {
        val level0 = height / dirBlockSizeL1 * dirBlockSizeL1
        val level1 = height / dirBlockSizeL2 * dirBlockSizeL2

        return listOf(
                parentDir,
                rangePadded(level0), "/",
                rangePadded(level1), "/",
                rangePadded(height),
                ".",
                type,
                version,
                ".avro"
        ).joinToString("")
    }

    fun rangePadded(block: Long): String {
        return StringUtils.leftPad(block.toString(), 9, "0")
    }
}