package io.emeraldpay.dshackle.archive

import com.fasterxml.jackson.annotation.JsonValue
import java.util.*

enum class FileType {
    BLOCKS,
    TRANSACTIONS;

    companion object {
        fun fromFilenameType(type: String): FileType? {
            return when (type.lowercase(Locale.getDefault())) {
                "txes", "transactions" -> TRANSACTIONS
                "block", "blocks" -> BLOCKS
                else -> null
            }
        }
    }

    @JsonValue
    open fun toLowerCase(): String {
        return toString().lowercase(Locale.getDefault())
    }
}