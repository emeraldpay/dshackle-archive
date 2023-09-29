package io.emeraldpay.dshackle.archive

import io.emeraldpay.api.BlockchainType
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
import org.slf4j.LoggerFactory
import org.springframework.boot.Banner
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.context.annotation.Import
import reactor.core.publisher.Mono
import reactor.tools.agent.ReactorDebugAgent
import java.lang.management.ManagementFactory
import java.time.Duration
import java.util.Locale
import kotlin.concurrent.thread
import kotlin.system.exitProcess
import kotlin.time.toKotlinDuration

@SpringBootApplication(scanBasePackages = ["io.emeraldpay.dshackle.archive"])
@Import(Config::class)
open class App

fun main(args: Array<String>) {
    val log = LoggerFactory.getLogger(App::class.java)
    val config = RunConfigInitializer().create(args) ?: return
    RunConfigHolder.value = config

    log.info("Run: ${config.command}")
    log.info("Run arguments: {}", args.joinToString(" "))

    ReactorDebugAgent.init()

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
    if (config.useS3()) {
        profiles.add("with-s3")
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
    try {
        runner.block()
        log.info("Attempting to close the context")
        ctx.close()
    } finally {
        log.info("Done: ${config.command}")
        val bean = ManagementFactory.getThreadMXBean()
        val infos = bean.dumpAllThreads(true, true)
        val runningThreads = infos.filter { it.threadName != "main" && !it.isDaemon }
        if (runningThreads.isNotEmpty()) {
            log.warn("Some threads ({}) are still running and prevent application to exit: \n{}", runningThreads.size, runningThreads.map { it.toString() })
            // make sure it exits after the completion even if there are still running threads
            thread(isDaemon = true) {
                val waitTime = Duration.ofSeconds(10)
                log.warn("Wait for {} before calling exit", waitTime.toKotlinDuration().toString())
                Thread.sleep(waitTime.toMillis())
                log.warn("Calling exit")
                exitProcess(0)
            }
        }
    }
}
