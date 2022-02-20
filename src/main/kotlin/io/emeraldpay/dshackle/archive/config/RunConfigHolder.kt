package io.emeraldpay.dshackle.archive.config

import org.slf4j.LoggerFactory

class RunConfigHolder {

    companion object {
        private val log = LoggerFactory.getLogger(RunConfigHolder::class.java)
        var value: RunConfig = RunConfig.default()
    }

}