package io.emeraldpay.dshackle.archive

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
import spock.lang.Specification

@SpringBootTest(args = ["-b=BITCOIN", "--range=700_000..799_999", "--dir=src/test/resources/playground", "--inputs=src/test/resources/playground/", "compact"])
@ActiveProfiles("run-compact")
@ComponentScan("io.emeraldpay.dshackle.archive.runner")
class RunCompactionSpec extends Specification {

    @Autowired
    RunCompaction runCompaction


    def setupSpec() {
        String[] args = [
                "-b=BITCOIN",
                "--range=700_000..799_999",
                "--dir=src/test/resources/playground",
                "--rangeChunk=5",
                "--inputs=src/test/resources/playground/",
                "compact"
        ]
        def config = new RunConfigInitializer().create(args)
        RunConfigHolder.value = config
    }

    def "Successful compaction"() {
        setup:
        def playGround = new File("src/test/resources/playground")
        playGround.listFiles().each { it.delete() }
        def fullAvroFiles = new File("src/test/resources/fullAvroFiles/")
        playGround.deleteDir()
        playGround.mkdir()
        FileUtils.copyDirectory(fullAvroFiles, playGround)
        runCompaction.run()
        expect:
        new File("src/test/resources/playground/btc/000700000/range-000723745_000723749.block.avro").exists()
        new File("src/test/resources/playground/btc/000700000/range-000723745_000723749.txes.avro").exists()
        new File("src/test/resources/playground/btc/000700000/range-000723755_000723759.block.avro").exists()
        new File("src/test/resources/playground/btc/000700000/range-000723755_000723759.txes.avro").exists()

        def block50_54 = new File("src/test/resources/playground/btc/000700000/range-000723750_000723754.block.avro")
        assert (block50_54.exists())

        def reader = new GenericDatumReader()
        def fileReader = new DataFileReader(block50_54, reader)
        def record = new GenericData.Record(fileReader.schema)
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

        !new File("src/test/resources/playground/000723753.block.avro").exists()
    }

    def "Unsuccessful compaction"() {
        setup:
        def playGround = new File("src/test/resources/playground")
        playGround.listFiles().each { it.delete() }
        def fullAvroFiles = new File("src/test/resources/fullAvroFiles/")
        playGround.deleteDir()
        playGround.mkdir()
        FileUtils.copyDirectory(fullAvroFiles, playGround)
        new File("src/test/resources/playground/000723751.block.avro").delete()
        expect:
        when:
        runCompaction.run()
        then:
        thrown Exception
    }

}
