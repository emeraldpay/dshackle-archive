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


@SpringBootTest(args = ["-b=BITCOIN", "--range=700_000..799_999", "--dir=src/test/resources/playground", "--inputs=src/test/resources/playground/", "compact"])
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
                "-b=BITCOIN",
                "--range=700_000..799_999",
                "--dir=src/test/resources/playground",
                "--rangeChunk=5",
                "--inputs=src/test/resources/playground/",
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

        assert(File("src/test/resources/playground/btc/000700000/range-000723745_000723749.block.avro").exists())
        assert(File("src/test/resources/playground/btc/000700000/range-000723745_000723749.txes.avro").exists())
        assert(File("src/test/resources/playground/btc/000700000/range-000723755_000723759.block.avro").exists())
        assert(File("src/test/resources/playground/btc/000700000/range-000723755_000723759.txes.avro").exists())

        val block50_54 = File("src/test/resources/playground/btc/000700000/range-000723750_000723754.block.avro")
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

        val block60_64 = File("src/test/resources/playground/btc/000700000/range-000723760_000723764.block.avro")
        assert(block60_64.exists())

        val reader3: DatumReader<GenericRecord> = GenericDatumReader()
        val fileReader3 = DataFileReader(block60_64, reader3)
        val record3: GenericRecord = GenericData.Record(fileReader3.schema)
        fileReader3.next(record3)
        assert(record3["height"].toString() == "723760")
        fileReader3.next(record3)
        assert(record3["height"].toString() == "723761")
        assert(!fileReader3.hasNext())
        fileReader3.close()

        val block40_44 = File("src/test/resources/playground/btc/000700000/range-000723740_000723744.block.avro")
        assert(block40_44.exists())

        val reader2: DatumReader<GenericRecord> = GenericDatumReader()
        val fileReader2 = DataFileReader(block40_44, reader2)
        val record2: GenericRecord = GenericData.Record(fileReader2.schema)
        fileReader2.next(record2)
        assert(record2["height"].toString() == "723743")
        fileReader2.next(record2)
        assert(record2["height"].toString() == "723744")
        assert(!fileReader2.hasNext())
        fileReader2.close()

        assert(!File("src/test/resources/playground/000723753.block.avro").exists())
    }

    @Test
    fun unsuccessfulCompaction() {
        File("src/test/resources/playground/000723751.block.avro").delete()
        assertThrows<Exception> { runCompaction.run() }
    }

}