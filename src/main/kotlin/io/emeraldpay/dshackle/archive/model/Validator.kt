package io.emeraldpay.dshackle.archive.model

interface Validator<T> {

    fun validate(value: T): String?

}