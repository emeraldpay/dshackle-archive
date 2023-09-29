package io.emeraldpay.dshackle.archive.storage

data class ListQuery(
    val prefix: String,
    val rangeStart: String,
    val rangeEnd: String? = null,
)
