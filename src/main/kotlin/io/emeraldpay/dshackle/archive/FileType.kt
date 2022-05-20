package io.emeraldpay.dshackle.archive

import com.fasterxml.jackson.annotation.JsonValue
import java.util.*

enum class FileType {
    BLOCKS,
    TRANSACTIONS;

    @JsonValue
    open fun toLowerCase(): String {
        return toString().lowercase(Locale.getDefault())
    }
}