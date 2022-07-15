package io.emeraldpay.dshackle.archive.config

import io.emeraldpay.grpc.Chain
import java.nio.file.Files
import java.util.*
import java.util.regex.Pattern
import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.CommandLineParser
import org.apache.commons.cli.DefaultParser
import org.apache.commons.cli.HelpFormatter
import org.apache.commons.cli.Option
import org.apache.commons.cli.Options
import org.apache.commons.cli.ParseException
import org.slf4j.LoggerFactory

class RunConfigInitializer {

    companion object {
        private val log = LoggerFactory.getLogger(RunConfigInitializer::class.java)
    }

    fun create(args: Array<String>): RunConfig? {
        val options = Options()

        Option("h", "help", false, "Show Help").let {
            it.isRequired = false
            options.addOption(it)
        }

        Option("b", "blockchain", true, "Blockchain").let {
            it.isRequired = false
            options.addOption(it)
        }

        Option("c", "connection", true, "Connection (host:port)").let {
            it.isRequired = false
            options.addOption(it)
        }

        Option(null, "connection.notls", false, "Disable TLS").let {
            it.isRequired = false
            options.addOption(it)
        }

        Option("r", "range", true, "Blocks Range (N...M)").let {
            it.isRequired = false
            options.addOption(it)
        }
        Option(null, "rangeChunk", true, "Range chunk size (default 1000)").let {
            it.isRequired = false
            options.addOption(it)
        }
        Option(null, "continue", false, "Continue from the last file if set").let {
            it.isRequired = false
            options.addOption(it)
        }

        Option("d", "dir", true, "Target directory").let {
            it.isRequired = false
            options.addOption(it)
        }
        Option(null, "dirBlocks", true, "How many blocks keep per subdirectory").let {
            it.isRequired = false
            options.addOption(it)
        }

        Option(null, "prefix", true, "File prefix").let {
            it.isRequired = false
            options.addOption(it)
        }

        Option("i", "inputs", true, "Input File(s). Accepts a glob pattern for filename").also {
            it.isRequired = false
            options.addOption(it)
        }

        Option(null, "include", true, "Include optional details in JSON (trace, stateDiff)").also {
            it.isRequired = false
            options.addOption(it)
        }

        Option(null, "tail", true, "Last T block to ensure are archived in streaming mode").also {
            it.isRequired = false
            options.addOption(it)
        }

        Option(null, "authGCP", true, "Path to GCP Authentication JSON").also {
            it.isRequired = false
            options.addOption(it)
        }

        Option(null, "notify.dir", true, "Write notifications as JSON line to the specified dir in a file <dshackle-archive-%STARTTIME.jsonl>").also {
            it.isRequired = false
            options.addOption(it)
        }

        Option(null, "notify.pubsub", true, "Send notifications as JSON to the specified Google Pubsub topic").also {
            it.isRequired = false
            options.addOption(it)
        }

        val parser: CommandLineParser = DefaultParser()
        val formatter = HelpFormatter()
        val cmd: CommandLine = try {
            parser.parse(options, args)
        } catch (e: ParseException) {
            printHelp(formatter, options)
            throw IllegalStateException(e)
        }

        if (cmd.hasOption("help")) {
            printHelp(formatter, options)
            return null
        }

        if (!cmd.hasOption("blockchain")) {
            System.err.println("Please specify target blockchain with --blockchain option")
            return null
        }

        val blockchain = cmd.getOptionValue("blockchain").let {
            Chain.valueOf(it.uppercase(Locale.getDefault()).replace("-", "_"))
        }

        val command: RunConfig.Command = cmd.argList.let {
            if (it.size > 1) {
                printHelp(formatter, options)
                return null
            }
            if (it.isEmpty()) "archive" else it.first()
        }.uppercase(Locale.getDefault()).let {
            RunConfig.Command.valueOf(it)
        }

        val accessBlockchain = listOf(
                RunConfig.Command.ARCHIVE,
                RunConfig.Command.STREAM,
                RunConfig.Command.FIX
        ).contains(command)

        val connection: RunConfig.Connection? = if (accessBlockchain) {
            cmd.getOptionValue("connection").let {
                if (it.contains(":")) {
                    it.split(":").let { parts ->
                        if (parts.size != 2) {
                            throw IllegalStateException("Invalid format for connection address: $it")
                        }
                        Pair<String, Int>(parts[0], parts[1].toInt())
                    }
                } else {
                    Pair(it, 2448)
                }
            }.let {
                RunConfig.Connection(it.first, it.second)
            }.let {
                if (cmd.hasOption("connection.notls")) {
                    it.copy(useTls = false)
                } else it
            }
        } else null

        var files = RunConfig.Files()
        cmd.getOptionValue("dir")?.let {
            files = files.copy(dir = it)
        }
        cmd.getOptionValue("prefix")?.let {
            files = files.copy(prefix = it)
        }
        cmd.getOptionValue("dirBlocks")?.toLong().let {
            if (it != null) {
                files = files.copy(dirBlockSizeL1 = it, dirBlockSizeL2 = it / 100)
            } else if (blockchain == Chain.BITCOIN) {
                files = files.copy(dirBlockSizeL1 = 100_000, dirBlockSizeL2 = 1000)
            }
        }

        var exportGS: RunConfig.ExportGS? = null
        if (isGSPath(files.dir)) {
            exportGS = extractGSConfig(files.dir)
            cmd.getOptionValue("authGCP")?.let {
                exportGS = exportGS!!.copy(credentials = it)
            }
            files = files.copy(
                    Files.createTempDirectory("emerald-dshackle-archive").toString()
            )
        }

        val archiveOptions = cmd.getOptionValue("include")?.let {
            val targets = it.split(",").map(String::trim).map(String::toLowerCase)
            RunConfig.ArchiveOptions(
                    trace = targets.contains("trace"),
                    stateDiff = targets.contains("statediff") || targets.contains("state")
            )
        } ?: RunConfig.ArchiveOptions()

        var range: RunConfig.Range = cmd.getOptionValue("range")?.let {
            RunConfig.Range.parse(it)
        } ?: RunConfig.Range.default()
        cmd.getOptionValue("rangeChunk")?.let { value ->
            range = range.copy(
                    chunk = value.toLong()
            )
        }
        if (cmd.hasOption("continue")) {
            range = range.copy(continueFromLast = true)
        }
        if (command == RunConfig.Command.STREAM) {
            range = range.copy(
                    individual = true
            )
            cmd.getOptionValue("tail")?.let {
                range = range.copy(tail = it.toLong())
            }
        }

        var notify = RunConfig.Notify.default()
        if (cmd.hasOption("notify.dir")) {
            notify = notify.copy(directory = cmd.getOptionValue("notify.dir"))
        }
        if (cmd.hasOption("notify.pubsub")) {
            notify = notify.copy(pubsub = cmd.getOptionValue("notify.pubsub"))
        }

        return RunConfig(command, blockchain, connection, archiveOptions, range, files, notify=notify).let { config ->
            if (command == RunConfig.Command.COPY || command == RunConfig.Command.COMPACT) {
                val inputs = cmd.getOptionValues("inputs").flatMap {
                    it.split(",")
                }.map {
                    it.trim()
                }
                config.copy(inputFiles = RunConfig.InputFiles(inputs))
            } else {
                config
            }
        }.let {
            if (exportGS != null) {
                it.copy(export = RunConfig.Export(exportGS))
            } else {
                it
            }
        }
    }

    fun isGSPath(path: String): Boolean {
        return path.startsWith("gs://")
    }

    fun extractGSConfig(path: String): RunConfig.ExportGS {
        val p = Pattern.compile("^gs://([^/]+)(/(.+?)/?)?\$")
        val m = p.matcher(path)
        if (!m.matches()) {
            log.warn("Invalid Google Storage path: $path")
            throw IllegalStateException()
        }
        return RunConfig.ExportGS(
                m.group(1),
                m.group(3) ?: ""
        )
    }

    fun printHelp(formatter: HelpFormatter, options: Options) {
        val header = "Copy blockchain data into files for further analysis"
        val footer = "Available commands:\n" +
                " archive - the main operation, copies data from a blockchain to archive\n" +
                " stream  - append fresh blocks one by one to the archive\n" +
                " compact - merge individual block files into larger range files\n" +
                " copy    - copy/recover from existing archive by copying into a new one\n" +
                " report  - show summary on what is in archive for the specified range\n" +
                " fix     - fix archive by making new archives for missing chunks"

        formatter.printHelp("dshackle-archive [options] <command>", header, options, footer)
    }
}