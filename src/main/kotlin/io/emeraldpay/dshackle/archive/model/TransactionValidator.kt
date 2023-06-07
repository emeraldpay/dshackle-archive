package io.emeraldpay.dshackle.archive.model

import io.emeraldpay.dshackle.archive.avro.BlockchainType
import io.emeraldpay.dshackle.archive.avro.Transaction
import io.emeraldpay.dshackle.archive.config.RunConfig
import java.nio.ByteBuffer
import org.slf4j.LoggerFactory

abstract class TransactionValidator: Validator<Transaction> {

    companion object {
        private val log = LoggerFactory.getLogger(TransactionValidator::class.java)
    }

    fun isEmpty(bytes: ByteBuffer?): Boolean {
        return bytes == null || bytes.limit() == 0
    }

    override fun validate(value: Transaction): String? {
        if (value.blockId == null) {
            return "no blockId"
        }
        if (value.height < 0) {
            return "invalid height ${value.height}"
        }
        if (value.txid == null) {
            return "not txid"
        }
        if (isEmpty(value.json)) {
            return "no json"
        }
        if (isEmpty(value.raw)) {
            return "no raw"
        }
        return null
    }

    class EthereumTransactionValidator(
            private val requireStateDiff: Boolean,
            private val requireTrace: Boolean,
    ): TransactionValidator() {
        override fun validate(value: Transaction): String? {
            val general = super.validate(value)
            if (general != null) {
                return general
            }
            if (isEmpty(value.receiptJson)) {
                return "no receipt"
            }
            if (requireStateDiff && isEmpty(value.stateDiffJson)) {
                return "no diff"
            }
            if (requireTrace && isEmpty(value.traceJson)) {
                return "no trace"
            }
            return null
        }
    }


    class BitcoinTransactionValidator(): TransactionValidator() {
        override fun validate(value: Transaction): String? {
            val general = super.validate(value)
            if (general != null) {
                return general
            }
            return null
        }
    }

    class Builder {
        private var blockchainType: BlockchainType? = null
        private var archiveOptions: RunConfig.ArchiveOptions? = null

        fun forBlockchain(blockchainType: BlockchainType): Builder {
            this.blockchainType = blockchainType
            return this
        }

        fun withOptions(archiveOptions: RunConfig.ArchiveOptions): Builder {
            this.archiveOptions = archiveOptions
            return this
        }

        fun build(): TransactionValidator {
            val blockchainType = this.blockchainType ?: throw NullPointerException("Blockchain Type is not set")
            return when (blockchainType) {
                BlockchainType.ETHEREUM -> {
                    val requireStateDiff = archiveOptions?.stateDiff ?: false
                    val requireTrace = archiveOptions?.trace ?: false
                    EthereumTransactionValidator(requireStateDiff, requireTrace)
                }
                BlockchainType.BITCOIN -> {
                    BitcoinTransactionValidator()
                }
                else -> {
                    throw IllegalStateException("Invalid blockchain type: $blockchainType")
                }
            }
        }
    }
}
