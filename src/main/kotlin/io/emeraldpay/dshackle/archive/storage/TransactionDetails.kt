package io.emeraldpay.dshackle.archive.storage

import java.nio.ByteBuffer

data class TransactionDetails(
    val hash: String,

    val raw: ByteBuffer,
    val json: ByteBuffer,

    val from: String? = null,
    val to: String? = null,

    val receiptJson: ByteBuffer? = null,
    val traceJson: ByteBuffer? = null,
    val stateDiff: ByteBuffer? = null,
)
