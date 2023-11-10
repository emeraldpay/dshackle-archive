package io.emeraldpay.dshackle.archive.config

import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Repository
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials

@Repository
@Profile("with-s3")
class AwsAuthProvider(
    @Autowired private val runConfig: RunConfig,
) {

    companion object {
        private val log = LoggerFactory.getLogger(AwsAuthProvider::class.java)
    }

    val credentials: AwsBasicCredentials
        get() {
            val auth = runConfig.auth.aws ?: throw IllegalStateException("No auth for AWS S3")
            return AwsBasicCredentials.create(
                auth.accessKey,
                auth.secretKey,
            )
        }
}
