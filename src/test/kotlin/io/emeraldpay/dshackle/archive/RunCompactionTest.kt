package io.emeraldpay.dshackle.archive

import io.emeraldpay.dshackle.archive.config.RunConfigHolder
import io.emeraldpay.dshackle.archive.config.RunConfigInitializer
import io.emeraldpay.dshackle.archive.runner.RunCompaction
import org.apache.avro.file.DataFileReader
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.DatumReader
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.assertThrows
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.ComponentScan
import org.springframework.test.context.ActiveProfiles
import java.io.File
import kotlin.test.Test


@SpringBootTest(args = ["-b=ETHEREUM", "--range=700_000..799_999", "--dir=src/test/resources/playground", "--inputs=src/test/resources/playground/", "compact"])
@ActiveProfiles("run-compact")
@ComponentScan("io.emeraldpay.dshackle.archive.runner")
class RunCompactionTest {
    @Autowired
    lateinit var runCompaction: RunCompaction

    companion object {
        @BeforeAll
        @JvmStatic
        fun init() {
            val args = arrayOf(
                "-b=ETHEREUM",
                "--range=700_000..799_999",
                "--dir=src/test/resources/playground",
                "--rangeChunk=5",
                "--inputs=src/test/resources/playground/*",
                "compact"
            )
            val config = RunConfigInitializer().create(args)
            RunConfigHolder.value = config
        }
    }

    @BeforeEach
    fun preparePlayGround() {
        val playGround = File("src/test/resources/playground")
        playGround.deleteRecursively()
        val fullAvroFiles = File("src/test/resources/fullAvroFiles/")
        fullAvroFiles.copyRecursively(playGround)
    }

    @Test
    fun successfulCompaction() {
        runCompaction.run()

        assert(File("src/test/resources/playground/range-000723745_000723749.block.avro").exists())
        assert(File("src/test/resources/playground/range-000723745_000723749.txes.avro").exists())
        assert(File("src/test/resources/playground/range-000723755_000723759.block.avro").exists())
        assert(File("src/test/resources/playground/range-000723755_000723759.txes.avro").exists())

        val block50_54 = File("src/test/resources/playground/range-000723750_000723754.block.avro")
        assert(block50_54.exists())

        val reader: DatumReader<GenericRecord> = GenericDatumReader()
        val fileReader = DataFileReader(block50_54, reader)
        val record: GenericRecord = GenericData.Record(fileReader.schema)
        fileReader.next(record)
        assert(record["height"].toString() == "723750")
        fileReader.next(record)
        assert(record["height"].toString() == "723751")
        fileReader.next(record)
        assert(record["height"].toString() == "723752")
        fileReader.next(record)
        assert(record["height"].toString() == "723753")
        fileReader.next(record)
        assert(record["height"].toString() == "723754")
        assert(!fileReader.hasNext())
        fileReader.close()
    }

    @Test
    fun unsuccessfulCompaction() {
        File("src/test/resources/playground/000723751.block.avro").delete()
        assertThrows<Exception> { runCompaction.run() }
    }

}