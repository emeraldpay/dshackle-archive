package io.emeraldpay.dshackle.archive.storage.gcp

import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.storage.BlobId
import com.google.cloud.storage.BlobInfo
import com.google.cloud.storage.Storage
import com.google.cloud.storage.StorageOptions
import io.emeraldpay.dshackle.archive.config.RunConfig
import io.emeraldpay.dshackle.archive.runner.PostArchiveHandler
import java.io.FileInputStream
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import javax.annotation.PostConstruct
import kotlin.io.path.fileSize
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Repository

@Repository
@Profile("with-gcp")
class UploadFiles(
        @Autowired private val runConfig: RunConfig,
        @Autowired private val googleStorage: GoogleStorage,
) : PostArchiveHandler {

    companion object {
        private val log = LoggerFactory.getLogger(UploadFiles::class.java)
    }

    private val uploads = Executors.newScheduledThreadPool(4)

    private val export: RunConfig.ExportGS = runConfig.export.gs!!

    private val bucket = export.bucket
    private val bucketPath = export.path.let { if (it.endsWith("/")) it.substring(0, it.length - 2) else it }
    private val baseDir = Path.of(runConfig.files.dir)

    fun getRelativePath(f: Path): String {
        return baseDir.relativize(f).toString()
    }

    fun getBucketPath(path: String): String {
        return listOf(
                bucketPath, path
        ).filter { it.isNotEmpty() }.joinToString("/")
    }

    override fun close() {
        uploads.shutdown()
        uploads.awaitTermination(60, TimeUnit.MINUTES)
    }


    override fun handle(f: Path) {
        val targetPath = getBucketPath(getRelativePath(f))
        val job = UploadJob(
                googleStorage.storage, bucket, targetPath, f,
                tries = 3,
                retry = {
                    if (it.tries > 0) {
                        schedule(it.copy(tries = it.tries - 1), Duration.ofSeconds(15))
                    }
                }
        )
        schedule(job, Duration.ofMillis(100))
    }

    fun schedule(job: UploadJob, delay: Duration) {
        uploads.schedule(job, delay.toMillis(), TimeUnit.MILLISECONDS)
    }

    data class UploadJob(
            private val storage: Storage,
            private val bucket: String,
            private val targetPath: String,
            private val source: Path,
            val tries: Int = 3,
            private val retry: (self: UploadJob) -> Unit
    ) : Runnable {
        val total = source.fileSize()

        private fun upload(): Boolean {
            val blobId = BlobId.of(bucket, targetPath)
            val blobInfo = BlobInfo.newBuilder(blobId).build()
            log.debug("Uploading ${blobId.toGsUtilUri()}")
            try {
                storage.writer(blobInfo, Storage.BlobWriteOption.disableGzipContent()).use { wrt ->
                    val input = FileChannel.open(source)
                    var position = 0L
                    while (position < total) {
                        position += input.transferTo(position, Long.MAX_VALUE, wrt)
                    }
                }
            } catch (t: Throwable) {
                retry(this)
                return false
            }

            return true
        }

        private fun cleanup() {
            Files.deleteIfExists(source)
        }

        override fun run() {
            if (upload()) {
                cleanup()
            }
        }
    }
}