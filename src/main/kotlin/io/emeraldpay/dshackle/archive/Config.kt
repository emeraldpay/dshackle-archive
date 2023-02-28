package io.emeraldpay.dshackle.archive

import com.fasterxml.jackson.core.Version
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import io.emeraldpay.api.EmeraldConnection
import io.emeraldpay.api.blockchain.BlockchainApi
import io.emeraldpay.dshackle.archive.config.RunConfig
import io.emeraldpay.dshackle.archive.config.RunConfigHolder
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile
import java.text.SimpleDateFormat
import java.util.TimeZone

@Configuration
open class Config {

    companion object {
        private val log = LoggerFactory.getLogger(Config::class.java)
    }

    @Bean
    fun runConfig(): RunConfig {
        return RunConfigHolder.value
    }

    @Bean
    @Profile("run-archive", "run-stream", "run-fix", "run-verify")
    fun dshackleClient(runConfig: RunConfig): BlockchainApi? {
        val connectionConfig = runConfig.connection ?: return null
        log.info("Connect to ${connectionConfig.host}:${runConfig.connection.port}")
        val connection = EmeraldConnection.newBuilder()
                .connectTo(
                        runConfig.connection.host,
                        runConfig.connection.port
                )
                .let {
                    if (!connectionConfig.useTls) {
                        it.usePlaintext()
                    } else {
                        it
                    }
                }
                // Some Trace JSONs are really huge. 100mb and more are common, sometimes even 1gb+.
                // We make it here to accept up to 2gb. But in practice it's still may fail.
                // For example, with default memory config some of large messages are going to fail with
                // `java.lang.OutOfMemoryError: Java heap space` when copying from a Protobuf.
                // So the Dshackle Archive Java options must be tuned for such usage scenario.
                .maxMessageSize(Int.MAX_VALUE)
                .build()
        return BlockchainApi(connection)
    }

    @Bean
    open fun objectMapper(): ObjectMapper {
        val module = SimpleModule("TestModule", Version(1, 0, 0, null, null, null))

        val objectMapper = ObjectMapper()
        objectMapper.registerModule(module)
        objectMapper.registerModule(Jdk8Module())
        objectMapper.registerModule(JavaTimeModule())
        objectMapper
                .setDateFormat(SimpleDateFormat("yyyy-MM-dd\'T\'HH:mm:ss.SSS\'Z\'"))
                .setTimeZone(TimeZone.getTimeZone("UTC"))

        return objectMapper
    }

}
