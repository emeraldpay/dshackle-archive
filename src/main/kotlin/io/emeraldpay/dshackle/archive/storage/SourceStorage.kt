package io.emeraldpay.dshackle.archive.storage

import io.emeraldpay.dshackle.archive.BlocksRange
import io.emeraldpay.dshackle.archive.config.RunConfig
import io.emeraldpay.dshackle.archive.storage.fs.FilesStorageAccess
import io.emeraldpay.dshackle.archive.storage.gcp.GSStorageAccess
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Service
import javax.annotation.PostConstruct

@Profile("run-compact", "run-copy", "run-report", "run-fix", "run-verify")
@Qualifier("sourceStorage")
@Service
open class SourceStorage(
    @Autowired private val runConfig: RunConfig,
    @Autowired private val allStorageAccess: List<StorageAccess>,
    @Autowired private val blocksRange: BlocksRange,
    @Autowired private val filenameGenerator: FilenameGenerator,
) {

    companion object {
        private val log = LoggerFactory.getLogger(SourceStorage::class.java)
    }

    lateinit var current: StorageAccess

    @PostConstruct
    fun init() {
        val instance = if (allStorageAccess.size == 1) {
            allStorageAccess.first()
        } else {
            // when we do a COPY from one source to another
            val sourceIsGs = runConfig.inputFiles == null ||
                runConfig.inputFiles.files.all { it.startsWith("gs://") }
            if (sourceIsGs) {
                allStorageAccess.find {
                    it is GSStorageAccess
                }
            } else {
                allStorageAccess.find {
                    it is FilesStorageAccess
                }
            }
        }
        if (instance == null) {
            log.error("Profile doesn't have Source Storage Access")
            throw IllegalStateException()
        }
        current = instance
    }

    fun getInputFiles(): StorageAccess.InputSources {
        if (runConfig.inputFiles == null) {
            throw IllegalStateException("List of input files is not set")
        }

        val range = blocksRange.wholeChunk()

        return current.inputSources(runConfig.inputFiles.files, range)
    }
}
