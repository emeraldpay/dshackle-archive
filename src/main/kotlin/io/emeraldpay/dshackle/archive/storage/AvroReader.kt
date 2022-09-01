package io.emeraldpay.dshackle.archive.storage

import org.apache.avro.file.SeekableInput
import org.reactivestreams.Publisher

interface AvroReader<T> {

    fun open(input: SeekableInput): Publisher<T>

}