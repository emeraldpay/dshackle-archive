package io.emeraldpay.dshackle.archive.storage.s3

import com.adobe.testing.s3mock.testcontainers.S3MockContainer
import reactor.core.publisher.Flux
import software.amazon.awssdk.http.SdkHttpConfigurationOption
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import software.amazon.awssdk.utils.AttributeMap;
import spock.lang.Specification

import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class S3PublisherIntegrationSpec extends Specification {

    S3MockContainer s3container = new S3MockContainer("3.1.0")
        .withInitialBuckets("test")
        .withVolumeAsRoot(File.createTempDir("dshackle-archive-test").toPath())

    S3Client storage

    def setup() {
        s3container.start()

        storage = S3Client.builder()
                .region(Region.of("us-east-1"))
                .credentialsProvider(
                        StaticCredentialsProvider.create(AwsBasicCredentials.create("foo", "bar")))
                .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
                .endpointOverride(URI.create(s3container.httpsEndpoint))
                .httpClient(UrlConnectionHttpClient.builder().buildWithDefaults(
                        AttributeMap.builder().put(SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES, Boolean.TRUE).build()))
                .build();
        uploadTestFiles()
    }

    def cleanup() {
        s3container.stop()
    }

    private def uploadTestFiles() {
        def uploader = Executors.newFixedThreadPool(4)
        Files.walk(Path.of("src/test/resources/fullAvroFiles/"), 5)
                .forEach { file ->
                    uploader.execute {
                        if (!Files.isDirectory(file)) {
                            def key = file.toString().substring("src/test/resources/fullAvroFiles/".length())
                            PutObjectRequest req = PutObjectRequest.builder()
                                    .bucket("test")
                                    .key(key)
                                    .build()
                            def result = storage.putObject(req, file)
                            println("Uploaded: ${key}")
                        }
                    }
                }
        uploader.shutdown()
        uploader.awaitTermination(1, TimeUnit.MINUTES)
    }

    def "List all files"() {
        setup:

        when:
        def publisher = new S3Publisher(storage, "test", "", null, null)
        def files = Flux.from(publisher)
                .collectList().block(Duration.ofSeconds(1))

        then:
        files.size() == 24
    }

    def "List ETH files"() {
        setup:

        when:
        def publisher = new S3Publisher(storage, "test", "ethereum/", null, null)
        def files = Flux.from(publisher)
                .collectList().block(Duration.ofSeconds(1))

        then:
        files.size() == 2
    }

    def "List BTC files in range"() {
        setup:

        when:
        def publisher = new S3Publisher(storage, "test", "btc/", "btc/000700000/range-00072375", null)
        def files = Flux.from(publisher)
                .collectList().block(Duration.ofSeconds(1))

        then:
        files.size() == 2
    }
}
