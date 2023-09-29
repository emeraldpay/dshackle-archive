package io.emeraldpay.dshackle.archive.storage.s3


import org.apache.avro.file.SeekableInput
import org.slf4j.LoggerFactory
import software.amazon.awssdk.core.ResponseInputStream
import software.amazon.awssdk.services.s3.model.GetObjectResponse
import software.amazon.awssdk.services.s3.model.S3Object
import java.io.InputStream

class SeekableS3Object(
    obj: ResponseInputStream<GetObjectResponse>,
): SeekableInput {

    companion object {
        private val log = LoggerFactory.getLogger(SeekableS3Object::class.java)
    }

    private val size = obj.response().contentLength()
    private val input: InputStream = obj
    private var pos: Long = 0

    override fun close() {
        input.close()
    }

    override fun seek(p: Long) {
        if (p < pos) {
            throw UnsupportedOperationException("Seeking backwards is not supported")
        }
        val skip = p - pos
        input.skip(skip)
        pos = p
    }

    override fun tell(): Long {
        return pos
    }

    override fun length(): Long {
        return size
    }

    override fun read(b: ByteArray?, off: Int, len: Int): Int {
        if (b == null) {
            throw NullPointerException()
        }
        return input.read(b, off, len).also {
            if (it > 0) {
                pos += it
            }
        }
    }

}
