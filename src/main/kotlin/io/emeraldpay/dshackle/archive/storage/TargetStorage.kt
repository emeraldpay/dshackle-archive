package io.emeraldpay.dshackle.archive.storage

import io.emeraldpay.dshackle.archive.config.RunConfig
import io.emeraldpay.dshackle.archive.storage.fs.FilesStorageAccess
import io.emeraldpay.dshackle.archive.storage.gcp.GSStorageAccess
import java.nio.file.Path
import javax.annotation.PostConstruct
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

@Service
@Qualifier("targetStorage")
class TargetStorage(
        @Autowired private val runConfig: RunConfig,
        @Autowired private val allStorageAccess: List<StorageAccess>,
        @Autowired private val filenameGenerator: FilenameGenerator,
) {

    companion object {
        private val log = LoggerFactory.getLogger(TargetStorage::class.java)
    }

    private lateinit var current: StorageAccess

    @PostConstruct
    fun init() {
        val instance = if (runConfig.useGCP()) {
            allStorageAccess.find {
                it is GSStorageAccess
            }
        } else {
            allStorageAccess.find {
                it is FilesStorageAccess
            }
        }
        if (instance == null) {
            log.error("Profile doesn't have Target Storage Access")
            throw IllegalStateException()
        }
        current = instance
    }

    fun locationFor(file: Path): String {
        val path = file.toFile().path
        val cleanPath = if (runConfig.useGCP()) {
            //TODO workaround for the local temp files which are used before uploading to the GCP
            path.substring(runConfig.files.dir.length + 1)
        } else {
            path
        }
        return current.locationFor(cleanPath)
    }

    fun listArchive(height: List<Long>?): Flux<String> {
        return current.listArchive(height)
    }

    fun deleteArchives(files: List<String>): Mono<Void> {
        return current.deleteArchives(files)
    }

}