package io.emeraldpay.dshackle.archive

import io.emeraldpay.dshackle.archive.config.RunConfig
import io.emeraldpay.dshackle.archive.config.RunConfigHolder
import io.emeraldpay.dshackle.archive.config.RunConfigInitializer
import io.emeraldpay.dshackle.archive.runner.RunArchive
import io.emeraldpay.dshackle.archive.runner.RunCompaction
import io.emeraldpay.dshackle.archive.runner.RunCopy
import io.emeraldpay.dshackle.archive.runner.RunFix
import io.emeraldpay.dshackle.archive.runner.RunReport
import io.emeraldpay.dshackle.archive.runner.RunStream
import io.emeraldpay.dshackle.archive.runner.RunVerify
import io.emeraldpay.grpc.BlockchainType
import java.util.*
import kotlin.collections.ArrayList
import kotlin.system.exitProcess
import org.slf4j.LoggerFactory
import org.springframework.boot.Banner
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.context.annotation.Import
import reactor.core.publisher.Mono

@SpringBootApplication(scanBasePackages = ["io.emeraldpay.dshackle.archive"])
@Import(Config::class)
open class App

fun main(args: Array<String>) {
    val log = LoggerFactory.getLogger(App::class.java)
    val config = RunConfigInitializer().create(args) ?: return
    RunConfigHolder.value = config

    log.info("Run: ${config.command}")

    val app = SpringApplication(App::class.java)
    app.setBannerMode(Banner.Mode.OFF)

    val profiles = ArrayList<String>()
    profiles.add("run-" + config.command.name.lowercase(Locale.getDefault()))

    when (BlockchainType.from(config.blockchain)) {
        BlockchainType.ETHEREUM -> profiles.add("ethereum")
        BlockchainType.BITCOIN -> profiles.add("bitcoin")
        else -> throw IllegalStateException("Unsupported blockchain: ${config.blockchain}")
    }

    if (config.useGCP()) {
        profiles.add("with-gcp")
    }

    if (config.connection != null) {
        profiles.add("with-blockchain")
    }

    app.setAdditionalProfiles(*profiles.toTypedArray())
    val ctx = app.run(*args)
    val runner: Mono<Void> = when (config.command) {
        RunConfig.Command.ARCHIVE -> ctx.getBean(RunArchive::class.java).run()
        RunConfig.Command.COPY -> ctx.getBean(RunCopy::class.java).run()
        RunConfig.Command.STREAM -> ctx.getBean(RunStream::class.java).run()
        RunConfig.Command.COMPACT -> ctx.getBean(RunCompaction::class.java).run()
        RunConfig.Command.REPORT -> ctx.getBean(RunReport::class.java).run()
        RunConfig.Command.FIX -> ctx.getBean(RunFix::class.java).run()
        RunConfig.Command.VERIFY -> ctx.getBean(RunVerify::class.java).run()
    }
    runner.block()
    // make sure it exits after the completion even if there are still running threads
    exitProcess(0)
}