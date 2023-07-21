package io.emeraldpay.dshackle.archive.runner

import io.emeraldpay.dshackle.archive.config.RunConfigHolder
import io.emeraldpay.dshackle.archive.config.RunConfigInitializer
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

@SpringBootTest
@ActiveProfiles("run-compact")
@ComponentScan("io.emeraldpay.dshackle.archive.runner")
class RunCompactionIntegrationWithRangesSpec extends Specification {

    @Autowired
    RunCompaction runCompaction

    @Shared
    private Path testDir

    @Shared
    private File playground

    def setupSpec() {
        //use a temp dir
        testDir = Files.createTempDirectory("RunCompactionIntegrationSpec-")
        playground = testDir.toFile()

        Files.createDirectories(testDir)

        def fullAvroFiles = new File("src/test/resources/fullAvroFiles/")
        FileUtils.copyDirectory(fullAvroFiles, playground)

        String[] args = [
                "-b=BITCOIN",
                // range bounds are in the middle of existed ranges,
                // right bound is excluded, so last block is 723_758
                "--range=723_746..723_759",
                "--dir=" + testDir.toString(),
                "--rangeChunk=7", // 723744 % 7 = 0
                "--compact.ranges", // enable ranges compact
                "--deduplicate",
                "--inputs=" + testDir.toString(),
                "compact"
        ]
        def config = new RunConfigInitializer().create(args)
        RunConfigHolder.value = config
    }

    def "Successful compaction with compact.ranges enabled"() {
        when:
        runCompaction.run().block()

        then:
        // chunk before range as 000723745_000723749 was split
        new File(playground, "btc/000700000/range-000723745_000723745.blocks.avro").exists()
        new File(playground, "btc/000700000/range-000723745_000723745.txes.avro").exists()
        // it should be 723744_723750 according to rangeChunk=7, but --range=723_746..x
        new File(playground, "btc/000700000/range-000723746_000723750.blocks.avro").exists()
        new File(playground, "btc/000700000/range-000723746_000723750.txes.avro").exists()
        new File(playground, "btc/000700000/range-000723751_000723757.blocks.avro").exists()
        new File(playground, "btc/000700000/range-000723751_000723757.txes.avro").exists()
        // --range bounds it to 723758
        new File(playground, "btc/000700000/range-000723758_000723758.blocks.avro").exists()
        new File(playground, "btc/000700000/range-000723758_000723758.txes.avro").exists()
        // the rest part of 723755_723759
        new File(playground, "btc/000700000/range-000723759_000723759.blocks.avro").exists()
        new File(playground, "btc/000700000/range-000723759_000723759.txes.avro").exists()
        when:
        def block51_57 = new File(playground, "btc/000700000/range-000723751_000723757.blocks.avro")

        then:
        block51_57.exists()

        when:
        def reader = new GenericDatumReader()
        def fileReader = new DataFileReader(block51_57, reader)
        def record = new GenericData.Record(fileReader.schema)

        then:
        fileReader.next(record)
        record["height"].toString() == "723751"
        fileReader.next(record)
        record["height"].toString() == "723752"
        fileReader.next(record)
        record["height"].toString() == "723753"
        fileReader.next(record)
        record["height"].toString() == "723754"
        fileReader.next(record)
        record["height"].toString() == "723755"
        fileReader.next(record)
        record["height"].toString() == "723756"
        fileReader.next(record)
        record["height"].toString() == "723757"
        !fileReader.hasNext()
        fileReader.close()

        new File(playground, "000723744.block.avro").exists()
        !new File(playground, "000723750.block.avro").exists()
        !new File(playground, "000723750.txes.avro").exists()
        !new File(playground, "000723751.block.avro").exists()
        !new File(playground, "000723751.txes.avro").exists()
        !new File(playground, "000723752.block.avro").exists()
        !new File(playground, "000723752.txes.avro").exists()
        !new File(playground, "000723753.block.avro").exists()
        !new File(playground, "000723753.txes.avro").exists()
        !new File(playground, "000723754.block.avro").exists()
        !new File(playground, "000723754.txes.avro").exists()
        !new File(playground, "range-000723745_000723749.blocks.avro").exists()
        !new File(playground, "range-000723745_000723749.txes.avro").exists()
        !new File(playground, "range-000723755_000723759.blocks.avro").exists()
        !new File(playground, "range-000723755_000723759.txes.avro").exists()
        new File(playground, "000723760.block.avro").exists()

        when:
        def postChunk = new File(playground, "btc/000700000/range-000723759_000723759.blocks.avro")

        then:
        postChunk.exists()

        when:
        fileReader = new DataFileReader(postChunk, new GenericDatumReader())
        record = new GenericData.Record(fileReader.schema)

        then:
        fileReader.next(record)
        record["height"].toString() == "723759"
        !fileReader.hasNext()
        fileReader.close()
    }

}
