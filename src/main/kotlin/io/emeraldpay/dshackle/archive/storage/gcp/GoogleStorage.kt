package io.emeraldpay.dshackle.archive.storage.gcp

import com.google.cloud.storage.Storage
import com.google.cloud.storage.StorageOptions
import io.emeraldpay.dshackle.archive.config.GoogleAuthProvider
import io.emeraldpay.dshackle.archive.config.RunConfig
import io.emeraldpay.dshackle.archive.storage.BucketPath
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Repository
import javax.annotation.PostConstruct

@Repository
@Profile("with-gcp")
class GoogleStorage(
    @Autowired private val runConfig: RunConfig,
    @Autowired private val googleAuthProvider: GoogleAuthProvider,
) {

    companion object {
        private val log = LoggerFactory.getLogger(GoogleStorage::class.java)
    }

    private val export: RunConfig.ExportBucket = runConfig.export.bucket!!
    val bucket = export.bucket
    val bucketPath = BucketPath(
        export.path
            .let { if (it.endsWith("/")) it.substring(0, it.length - 2) else it }
            .trimStart('/'),
    )

    lateinit var storage: Storage

    @PostConstruct
    fun prepare() {
        val credentials = googleAuthProvider.credentials
        storage = StorageOptions.newBuilder()
            .setCredentials(credentials)
            .build().service

        log.info("Upload archives to Google Storage bucket $bucket into $bucketPath")
    }
}
