package io.emeraldpay.dshackle.archive.storage.gcp

import com.google.cloud.ReadChannel
import org.apache.avro.file.SeekableInput
import org.slf4j.LoggerFactory
import java.nio.ByteBuffer

class SeekableChannelInput(
    private val length: Long,
    private val channel: ReadChannel,
) : SeekableInput {

    companion object {
        private val log = LoggerFactory.getLogger(SeekableChannelInput::class.java)
    }

    private var pos: Long = 0

    override fun close() {
        channel.close()
    }

    override fun seek(p: Long) {
        pos = p
        channel.seek(p)
    }

    override fun tell(): Long {
        return pos
    }

    override fun length(): Long {
        return length
    }

    override fun read(b: ByteArray, off: Int, len: Int): Int {
        val buf = ByteBuffer.wrap(b, off, len)
        val size = channel.read(buf)
        if (size > 0) {
            pos += size
        }
        return size
    }
}
