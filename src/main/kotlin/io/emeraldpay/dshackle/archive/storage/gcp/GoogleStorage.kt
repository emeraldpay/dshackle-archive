package io.emeraldpay.dshackle.archive.storage.gcp

import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.storage.Storage
import com.google.cloud.storage.StorageOptions
import io.emeraldpay.dshackle.archive.config.RunConfig
import java.io.FileInputStream
import javax.annotation.PostConstruct
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Repository

@Repository
@Profile("with-gcp")
class GoogleStorage(
        @Autowired private val runConfig: RunConfig,
) {

    companion object {
        private val log = LoggerFactory.getLogger(GoogleStorage::class.java)
    }

    private val export: RunConfig.ExportGS = runConfig.export.gs!!
    val bucket = export.bucket
    val bucketPath = export.path.let { if (it.endsWith("/")) it.substring(0, it.length - 2) else it }

    lateinit var storage: Storage

    @PostConstruct
    fun prepare() {
        val jsonPath = export.credentials
        val credentials: GoogleCredentials = (
                if (StringUtils.isNotEmpty(jsonPath)) {
                    log.info("Use GCP Credentials from: $jsonPath")
                    GoogleCredentials.fromStream(FileInputStream(jsonPath!!))
                } else {
                    log.warn("Using system default GCP Credentials")
                    GoogleCredentials.getApplicationDefault()
                }
                ).createScoped(listOf("https://www.googleapis.com/auth/cloud-platform"))


        storage = StorageOptions.newBuilder()
                .setCredentials(credentials)
                .build().service

        log.info("Upload archives to Google Storage bucket $bucket into $bucketPath")
    }
}