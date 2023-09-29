package io.emeraldpay.dshackle.archive.storage.s3

import com.adobe.testing.s3mock.testcontainers.S3MockContainer
import io.emeraldpay.dshackle.archive.config.RunConfig
import io.emeraldpay.dshackle.archive.storage.FilenameGenerator
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import spock.lang.Specification

class S3StorageAccessIntegrationSpec extends Specification {

    S3MockContainer s3container = new S3MockContainer("3.1.0")
            .withInitialBuckets("test")
            .withVolumeAsRoot(File.createTempDir("dshackle-archive-test").toPath())

    FilenameGenerator generator = new FilenameGenerator("", "test/", 1_000, 1_00)
    S3Config s3config
    S3StorageAccess storage

    def setup() {
        s3container.start()
        s3config = new S3Config(
                new RunConfig.ExportBucket("s3://test:${s3container.httpsServerPort}",
                        new RunConfig.S3Options("us-east-1", true, URI.create(s3container.httpsEndpoint), true)
                ),
                AwsBasicCredentials.create("foo", "bar")
        )
        s3config.prepare()
        storage = new S3StorageAccess(generator, s3config)
    }

    def cleanup() {
        s3container.stop()
    }

    def "Write a file"() {
        when:
        def writer = storage.createWriter("test.txt")
        writer.write("Hello World".bytes)
        writer.close()

        // mock is a bit slow to close the file, and it's unavailable immediately after the write. 500ms is not enough
        Thread.sleep(1000)

        def act = s3config.storage.getObject(
                GetObjectRequest.builder().bucket("test").key("test.txt").build()
        ).readAllBytes();

        then:
        act == "Hello World".bytes
    }

    def "Read a file"() {
        setup:
        s3config.storage.putObject(
                PutObjectRequest.builder().bucket("test").key("test.txt").build(),
                RequestBody.fromString("Hello World")
        )
        when:
        def input = storage.createReader("test.txt")
        input.seek(3)
        byte[] buf = new byte[8]
        input.read(buf, 0, 8)
        input.close()

        then:
        buf == "lo World".bytes
    }
}
