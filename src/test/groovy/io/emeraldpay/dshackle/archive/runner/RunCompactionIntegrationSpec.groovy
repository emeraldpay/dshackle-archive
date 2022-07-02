package io.emeraldpay.dshackle.archive.runner

import io.emeraldpay.dshackle.archive.config.RunConfigHolder
import io.emeraldpay.dshackle.archive.config.RunConfigInitializer
import io.emeraldpay.dshackle.archive.runner.RunCompaction
import org.apache.avro.file.DataFileReader
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericDatumReader
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.ComponentScan
import org.springframework.test.context.ActiveProfiles
import org.testcontainers.shaded.org.apache.commons.io.FileUtils
import spock.lang.Shared
import spock.lang.Specification

import java.nio.file.Files
import java.nio.file.Path

@SpringBootTest(args = ["-b=BITCOIN", "--range=700_000..799_999", "--dir=src/test/resources/playground", "--inputs=src/test/resources/playground/", "compact"])
@ActiveProfiles("run-compact")
@ComponentScan("io.emeraldpay.dshackle.archive.runner")
class RunCompactionIntegrationSpec extends Specification {

    @Autowired
    RunCompaction runCompaction

    @Shared
    private Path testDir = Path.of("./playground")

    //TODO use a temp dir
    @Shared
    private File playground = testDir.toFile()

    def setupSpec() {
        //TODO use a temp dir instead
        FileUtils.deleteDirectory(playground)
        Files.createDirectories(testDir)

        def fullAvroFiles = new File("src/test/resources/fullAvroFiles/")
        FileUtils.copyDirectory(fullAvroFiles, playground)

        String[] args = [
                "-b=BITCOIN",
                "--range=700_000..799_999",
                "--dir=./playground",
                "--rangeChunk=5",
                "--inputs=./playground/",
                "compact"
        ]
        def config = new RunConfigInitializer().create(args)
        RunConfigHolder.value = config
    }

    def "Successful compaction"() {
        when:
        runCompaction.run().block()

        then:
        new File(playground, "btc/000700000/range-000723745_000723749.blocks.avro").exists()
        new File(playground, "btc/000700000/range-000723745_000723749.txes.avro").exists()
        new File(playground, "btc/000700000/range-000723755_000723759.blocks.avro").exists()
        new File(playground, "btc/000700000/range-000723755_000723759.txes.avro").exists()

        when:
        def block50_54 = new File(playground, "btc/000700000/range-000723750_000723754.blocks.avro")

        then:
        block50_54.exists()

        when:
        def reader = new GenericDatumReader()
        def fileReader = new DataFileReader(block50_54, reader)
        def record = new GenericData.Record(fileReader.schema)

        then:
        fileReader.next(record)
        record["height"].toString() == "723750"
        fileReader.next(record)
        record["height"].toString() == "723751"
        fileReader.next(record)
        record["height"].toString() == "723752"
        fileReader.next(record)
        record["height"].toString() == "723753"
        fileReader.next(record)
        record["height"].toString() == "723754"
        !fileReader.hasNext()
        fileReader.close()

        !new File(playground, "000723753.block.avro").exists()
    }

}
