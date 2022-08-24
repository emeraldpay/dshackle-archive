package io.emeraldpay.dshackle.archive.model


/**
 * A dummy exception to throw when an original exception was processed (ex., provided a message to a user) but was rethrown just to stop the processing
 */
class ProcessedException(t: Throwable): RuntimeException(t)