package io.emeraldpay.dshackle.archive.storage

import io.emeraldpay.dshackle.archive.BlocksRange
import io.emeraldpay.dshackle.archive.config.RunConfig
import io.emeraldpay.dshackle.archive.runner.RunCopy
import io.emeraldpay.dshackle.archive.storage.fs.FilesStorageAccess
import io.emeraldpay.dshackle.archive.storage.gcp.GSStorageAccess
import java.io.File
import java.nio.file.Path
import javax.annotation.PostConstruct
import kotlin.io.path.name
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

@Profile("run-compact", "run-copy")
@Qualifier("sourceStorage")
@Service
class SourceStorage(
        @Autowired private val runConfig: RunConfig,
        @Autowired private val allStorageAccess: List<StorageAccess>,
        @Autowired private val blocksRange: BlocksRange,
        @Autowired private val filenameGenerator: FilenameGenerator,
) {

    companion object {
        private val log = LoggerFactory.getLogger(SourceStorage::class.java)
    }

    private lateinit var current: StorageAccess

    @PostConstruct
    fun init() {
        val sourceIsFiles = runConfig.inputFiles != null

        val instance = if (sourceIsFiles) {
            allStorageAccess.find {
                it is FilesStorageAccess
            }
        } else {
            allStorageAccess.find {
                it is GSStorageAccess
            }
        }
        if (instance == null) {
            log.error("Profile doesn't have Source Storage Access")
            throw IllegalStateException()
        }
        current = instance
    }

    fun getInputFiles(): InputSources {
        if (runConfig.inputFiles == null) {
            throw IllegalStateException("List of input files is not set")
        }

        val range = blocksRange.wholeChunk()
        val transactions = mutableListOf<Path>()
        val blocks = mutableListOf<Path>()

        runConfig.inputFiles.files.forEach { pattern ->
            File(pattern).walk()
                    .filter { file ->
                        val chunk = filenameGenerator.parseRange(file.name)
                        if (chunk == null) {
                            log.debug("Skip ${file.name}")
                        }
                        val accept = chunk != null && range.intersects(chunk)
                        if (!accept) {
                            log.trace("Skip ${file.name}")
                        }
                        accept
                    }
                    .sortedBy { file ->
                        val chunk = filenameGenerator.parseRange(file.name)
                        chunk!!.startBlock
                    }
                    .map { it.toPath() }
                    .forEach {
                        if (it.name.contains("transactions") || it.name.contains("txes")) {
                            transactions.add(it)
                        } else if (it.name.contains("blocks") || it.name.contains("block")) {
                            blocks.add(it)
                        } else {
                            log.warn("Unknown type of file: $it")
                        }
                    }
        }
        return InputSources(
                transactions = Flux.fromIterable(transactions),
                blocks = Flux.fromIterable(blocks)
        )
    }

    fun listArchive(height: List<Long>?): Flux<String> {
        return current.listArchive(height)
    }

    fun deleteArchives(files: List<String>): Mono<Void> {
        return current.deleteArchives(files)
    }

    data class InputSources(
            val transactions: Flux<Path>,
            val blocks: Flux<Path>,
    )
}