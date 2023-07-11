package io.emeraldpay.dshackle.archive.model

import io.emeraldpay.dshackle.archive.avro.Block
import io.emeraldpay.dshackle.archive.avro.BlockchainType
import org.slf4j.LoggerFactory

abstract class BlockValidation : Validator<Block> {

    companion object {
        private val log = LoggerFactory.getLogger(BlockValidation::class.java)
    }

    override fun validate(value: Block): String? {
        if (value.blockId.isEmpty()) {
            return "no blockId"
        }
        return null
    }

    class EthereumBlockValidator() : BlockValidation() {
        override fun validate(value: Block): String? {
            return super.validate(value)
        }
    }

    class BitcoinBlockValidator() : BlockValidation() {
        override fun validate(value: Block): String? {
            return super.validate(value)
        }
    }

    class Builder {
        private var blockchainType: BlockchainType? = null

        fun forBlockchain(blockchainType: BlockchainType): Builder {
            this.blockchainType = blockchainType
            return this
        }

        fun build(): BlockValidation {
            val blockchainType = this.blockchainType ?: throw NullPointerException("Blockchain Type is not set")
            return when (blockchainType) {
                BlockchainType.ETHEREUM -> {
                    EthereumBlockValidator()
                }

                BlockchainType.BITCOIN -> {
                    BitcoinBlockValidator()
                }

                else -> {
                    throw IllegalStateException("Invalid blockchain type: $blockchainType")
                }
            }
        }
    }
}
