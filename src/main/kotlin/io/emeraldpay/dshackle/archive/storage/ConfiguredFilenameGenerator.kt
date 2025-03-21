package io.emeraldpay.dshackle.archive.storage

import io.emeraldpay.dshackle.archive.BlocksRange
import io.emeraldpay.dshackle.archive.FileType
import io.emeraldpay.dshackle.archive.config.RunConfig
import io.emeraldpay.dshackle.archive.model.Chunk
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import java.util.EnumMap
import java.util.Locale

@Service
class ConfiguredFilenameGenerator(
    @Autowired private val runConfig: RunConfig,
    @Autowired private val range: BlocksRange,
) : FilenameGenerator(
    runConfig.files.prefix,
    runConfig.blockchain.chainCode.lowercase(Locale.getDefault()) + "/",
    runConfig.files.dirBlockSizeL1,
    runConfig.files.dirBlockSizeL2,
) {

    companion object {
        private val log = LoggerFactory.getLogger(ConfiguredFilenameGenerator::class.java)
    }

    private val fileTypes = EnumMap<FileType, String>(FileType::class.java).also {
        it[FileType.TRANSACTIONS] = "txes"
        it[FileType.BLOCKS] = if (runConfig.range.individual) {
            "block"
        } else {
            "blocks"
        }
    }

    fun fileForAutoRange(type: FileType, height: Long): String {
        if (range.isUsingRanges) {
            val chunk = range.getChunkAt(height)
            return fileFor(type, chunk)
        }
        return fileForIndividual(type, height)
    }

    fun fileForIndividual(type: FileType, height: Long): String {
        return getIndividualFilename(fileTypes[type]!!, height)
    }

    fun fileFor(type: FileType, height: Long): String {
        val chunk = range.getChunkAt(height)
        return fileFor(type, chunk)
    }

    fun fileFor(type: FileType, chunk: Chunk): String {
        return getRangeFilename(fileTypes[type]!!, chunk)
    }
}
