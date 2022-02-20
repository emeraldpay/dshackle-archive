package io.emeraldpay.dshackle.archive.storage

import io.emeraldpay.dshackle.archive.BlocksRange
import io.emeraldpay.dshackle.archive.config.RunConfig
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import java.nio.file.Files
import java.nio.file.Path
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

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

    private val dir = Path.of(runConfig.files.dir)
    private val fileManipulationLock = ReentrantLock()
    private val okFiles = ConcurrentHashMap<Path, Boolean>()


    init {
        Files.createDirectories(dir)
    }

    fun fileForAutoRange(type: String, height: Long, append: Boolean): Path {
        if (range.isUsingRanges) {
            val chunk = range.getChunkAt(height)
            return fileFor(type, chunk, append)
        }
        return fileForIndividual(type, height, append)
    }

    fun fileForIndividual(type: String, height: Long, append: Boolean): Path {
        val filename = getIndividualFilename(type, height)
        val path = dir.resolve(filename)
        return ensureFile(path, append)
    }

    fun fileFor(type: String, height: Long, append: Boolean): Path {
        val chunk = range.getChunkAt(height)
        return fileFor(type, chunk, append)
    }

    fun fileFor(type: String, chunk: BlocksRange.Chunk, append: Boolean): Path {
        val filename = getRangeFilename(type, chunk)
        val path = dir.resolve(filename)
        return ensureFile(path, append)
    }

    fun ensureFile(path: Path, append: Boolean): Path {
        if (okFiles.containsKey(path)) {
            return path
        }
        fileManipulationLock.withLock {
            val create: Boolean = if (!Files.exists(path)) {
                Files.createDirectories(path.parent)
                true
            } else if (!append) {
                Files.deleteIfExists(path)
                true
            } else {
                false
            }
            if (create) {
                Files.createFile(path)
            }
            okFiles.put(path, true)
        }
        return path
    }


}