package io.emeraldpay.dshackle.archive.storage

import io.emeraldpay.dshackle.archive.config.RunConfig
import io.emeraldpay.dshackle.archive.storage.fs.FilesStorageAccess
import io.emeraldpay.dshackle.archive.storage.gcp.GSStorageAccess
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Service
import javax.annotation.PostConstruct

@Service
@Qualifier("targetStorage")
class TargetStorage(
    @Autowired private val runConfig: RunConfig,
    @Autowired private val allStorageAccess: List<StorageAccess>,
) {

    companion object {
        private val log = LoggerFactory.getLogger(TargetStorage::class.java)
    }

    lateinit var current: StorageAccess

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
}
