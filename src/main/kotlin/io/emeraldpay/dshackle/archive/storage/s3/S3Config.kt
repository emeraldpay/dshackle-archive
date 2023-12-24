package io.emeraldpay.dshackle.archive.storage.s3

import io.emeraldpay.dshackle.archive.config.AwsAuthProvider
import io.emeraldpay.dshackle.archive.config.RunConfig
import io.emeraldpay.dshackle.archive.storage.BucketPath
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.FactoryBean
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Service
import software.amazon.awssdk.auth.credentials.AwsCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.http.SdkHttpConfigurationOption
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.S3Configuration
import software.amazon.awssdk.utils.AttributeMap
import javax.annotation.PostConstruct

class S3Config(
    export: RunConfig.ExportBucket,
    private val credentials: AwsCredentials,
) {

    @Service
    @Profile("with-s3")
    class S3ConfigBean(
        @Autowired private val runConfig: RunConfig,
        @Autowired private val awsAuthProvider: AwsAuthProvider,
    ) : FactoryBean<S3Config> {
        override fun getObject(): S3Config {
            return S3Config(runConfig.export.bucket!!, awsAuthProvider.credentials).also { it.prepare() }
        }

        override fun getObjectType(): Class<*>? {
            return S3Config::class.java
        }
    }

    companion object {
        private val log = LoggerFactory.getLogger(S3Config::class.java)
    }

    private val s3opts = export.s3!!
    val bucket = export.bucket
    val bucketPath = BucketPath(
        export.path
            .let { if (it.endsWith("/")) it.substring(0, it.length - 2) else it },
    )

    lateinit var storage: S3Client

    @PostConstruct
    fun prepare() {
        storage = S3Client
            .builder()
            .credentialsProvider(StaticCredentialsProvider.create(credentials))
            .region(Region.of(s3opts.region))
            .serviceConfiguration(
                S3Configuration
                    .builder()
                    .pathStyleAccessEnabled(s3opts.pathStyleAccess)
                    .build(),
            )
            .let {
                if (s3opts.endpoint != null) {
                    it.endpointOverride(s3opts.endpoint)
                } else {
                    it
                }
            }
            .httpClient(
                UrlConnectionHttpClient.builder()
                    .buildWithDefaults(
                        AttributeMap.builder()
                            .let {
                                if (s3opts.trustAnyTLS) {
                                    it.put(SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES, true)
                                } else {
                                    it
                                }
                            }
                            .build(),
                    ),
            )
            .build()

        log.info("Upload archives to S3 bucket `$bucket` into `$bucketPath`")
    }
}
