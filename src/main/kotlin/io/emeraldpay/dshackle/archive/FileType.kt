package io.emeraldpay.dshackle.archive

import com.fasterxml.jackson.annotation.JsonValue
import java.util.Locale

enum class FileType {
    BLOCKS,
    TRANSACTIONS,
    ;

    companion object {
        fun fromFilenameType(type: String): FileType? {
            return when (type.lowercase(Locale.getDefault())) {
                "txes", "transactions" -> TRANSACTIONS
                "block", "blocks" -> BLOCKS
                else -> null
            }
        }
    }

    fun asTypeMultiple(): String {
        return when (this) {
            TRANSACTIONS -> "txes"
            BLOCKS -> "blocks"
        }
    }

    fun asTypeSingle(): String {
        return when (this) {
            TRANSACTIONS -> "tx"
            BLOCKS -> "block"
        }
    }

    @JsonValue
    open fun toLowerCase(): String {
        return toString().lowercase(Locale.getDefault())
    }
}
