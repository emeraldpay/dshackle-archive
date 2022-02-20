package io.emeraldpay.dshackle.archive.storage

import java.nio.ByteBuffer
import java.time.Instant

data class BlockDetails(
        val timestamp: Instant,
        val height: Long,
        val hash: String,
        val parentHash: String,
        val transactionHashes: List<String>,
        val raw: ByteBuffer,
        val transactions: List<TransactionDetails>,

        // Ethereum only
        val uncles: List<ByteBuffer>? = null,
)