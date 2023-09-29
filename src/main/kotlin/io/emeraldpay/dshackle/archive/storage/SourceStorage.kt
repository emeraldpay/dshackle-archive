package io.emeraldpay.dshackle.archive.storage

import io.emeraldpay.dshackle.archive.BlocksRange
import io.emeraldpay.dshackle.archive.config.RunConfig
import io.emeraldpay.dshackle.archive.storage.fs.FilesStorageAccess
import io.emeraldpay.dshackle.archive.storage.gcp.GSStorageAccess
import io.emeraldpay.dshackle.archive.storage.s3.S3StorageAccess
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
            val sourceIsGs = runConfig.useGCP() || runConfig.inputFiles?.files?.all { it.startsWith("gs://") } == true
            val sourceIsS3 = runConfig.useS3() || runConfig.inputFiles?.files?.all { it.startsWith("s3://") } == true
            if (sourceIsGs) {
                allStorageAccess.find {
                    it is GSStorageAccess
                }
            } else if (sourceIsS3) {
                allStorageAccess.find {
                    it is S3StorageAccess
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
