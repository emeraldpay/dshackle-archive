package io.emeraldpay.dshackle.archive.config

import com.fasterxml.jackson.annotation.JsonValue
import io.emeraldpay.api.Chain
import io.emeraldpay.dshackle.archive.avro.BlockchainType
import java.net.URI
import java.time.Duration
import java.util.Locale

data class RunConfig(
    val command: Command,
    val blockchain: Chain,
    val connection: Connection?,
    val options: ArchiveOptions,
    val range: Range,
    val files: Files,
    val dryRun: Boolean = false,
    val inputFiles: InputFiles? = null,
    val export: Export = Export.default(),
    val notify: Notify = Notify.default(),
    val auth: Auth = Auth.default(),
    val deduplicate: Boolean = false,
    val compaction: CompactionOptions = CompactionOptions(),
) {
    companion object {
        @JvmStatic
        fun default(): RunConfig {
            return RunConfig(
                Command.ARCHIVE,
                Chain.ETHEREUM,
                Connection.default(),
                ArchiveOptions(),
                Range.default(),
                Files(),
                false,
                null,
            )
        }
    }

    val chainType: BlockchainType = when (io.emeraldpay.api.BlockchainType.from(blockchain)) {
        io.emeraldpay.api.BlockchainType.ETHEREUM -> BlockchainType.ETHEREUM
        io.emeraldpay.api.BlockchainType.BITCOIN -> BlockchainType.BITCOIN
        else -> throw IllegalStateException("Unsupported chain: $blockchain")
    }

    fun getChainId(): String {
        return blockchain.chainCode
    }

    fun useGCP(): Boolean {
        return export.bucket?.isGCP ?: false
    }

    fun useS3(): Boolean {
        return export.bucket?.isS3 ?: false
    }

    fun withRange(range: Range): RunConfig {
        return copy(range = range)
    }

    enum class Command {
        ARCHIVE,
        COPY,
        STREAM,
        COMPACT,
        REPORT,
        FIX,
        VERIFY,
        ;

        @JsonValue
        open fun toLowerCase(): String {
            return toString().lowercase(Locale.getDefault())
        }
    }

    data class Connection(
        val host: String,
        val port: Int,
        val useTls: Boolean = true,
        val timeout: Duration = Duration.ofSeconds(15),
        val parallel: Int = RunConfigInitializer.DEFAULT_PARALLEL,
    ) {
        companion object {
            fun default(): Connection {
                return Connection("127.0.0.1", 2448)
            }
        }

        fun describe(): String {
            return "$host:$port " + if (useTls) "(use TLS)" else "(no TLS)"
        }
    }

    data class ArchiveOptions(
        val trace: Boolean = false,
        val stateDiff: Boolean = false,
    )

    data class Range(
        val start: Long,
        val count: Long,
        val chunk: Long,
        val individual: Boolean,
        val tail: Long = 100,
        val continueFromLast: Boolean = false,
        val backward: Boolean = false,
        val aligned: Boolean = true,
    ) {
        companion object {
            private const val DEFAULT_CHUNK: Long = 1_000
            private const val DEFAULT_INDIVIDUAL: Boolean = false

            @JvmStatic
            fun default(): Range {
                return Range(0, 0, DEFAULT_CHUNK, DEFAULT_INDIVIDUAL)
            }

            private fun parseNumber(s: String): Long {
                return s.replace("_", "").trim().toLong()
            }

            fun parse(range: String): Range {
                return if (range.contains("..")) {
                    range.split("..").let {
                        Range(parseNumber(it[0]), parseNumber(it[1]) - parseNumber(it[0]), DEFAULT_CHUNK, DEFAULT_INDIVIDUAL)
                    }
                } else {
                    Range(parseNumber(range), 1, DEFAULT_CHUNK, true)
                }
            }

            @JvmStatic
            fun forRange(start: Long, count: Long, chunk: Long): Range {
                return Range(start, count, chunk, chunk == 1L)
            }
        }

        fun withContinueFromLast(value: Boolean): Range {
            return copy(continueFromLast = value)
        }

        fun withBackward(value: Boolean): Range {
            return copy(backward = value)
        }

        fun validate() {
            require(!continueFromLast || !backward) {
                "Only one of --continue or --back must be set"
            }
        }
    }

    data class Files(
        val dir: String = ".",
        val prefix: String = "",
        val dirBlockSizeL1: Long = 1_000_000,
        val dirBlockSizeL2: Long = 1_000,
    )

    data class Export(
        val bucket: ExportBucket? = null,
    ) {
        companion object {
            fun default(): Export {
                return Export(null)
            }
        }
    }

    data class ExportBucket(
        val uri: String,
        // specific to AWS
        val s3: S3Options? = null,
    ) {

        val isS3: Boolean
            get() = uri.startsWith("s3://")

        val isGCP: Boolean
            get() = uri.startsWith("gs://")

        val bucket: String
        val path: String

        init {
            val p = Regex("^(s3|gs)://([^/\\\\.:]+)(\\.[^/]+)?(:\\d+)?(/(.+?)/?)?\$")
            val m = p.matchEntire(uri)
            if (m != null) {
                bucket = m.groupValues[2]
                path = m.groupValues[5] ?: ""
            } else {
                throw IllegalArgumentException("Invalid storage URI: $uri")
            }
        }

        fun ensureS3(): ExportBucket {
            return if (isS3) {
                if (s3 != null) {
                    this
                } else {
                    this.copy(s3 = S3Options())
                }
            } else {
                this
            }
        }
    }

    data class S3Options(
        val region: String = "global",
        /**
         * There are two types of accessing S3: virtual host style and path style. First is BUCKET.HOST/PATH, and the second is HOST/BUCKET/PATH.
         * Non-AWS providers usually don't support the virtual hosts and should be accessed by HOST only, i.e. the path style.
         *
         * @link https://docs.aws.amazon.com/AmazonS3/latest/userguide/VirtualHosting.html
         */
        val pathStyleAccess: Boolean = false,
        /**
         * An HTTP endpoint host name to use if it's not an AWS endpoint
         */
        val endpoint: URI? = null,
        /**
         * Set to true for a custom signed certificates on the server side, otherwise it will fail on TLS validation
         */
        val trustAnyTLS: Boolean = false,
    )

    data class InputFiles(
        val files: List<String>,
    )

    data class Notify(
        val file: String? = null,
        val directory: String? = null,
        val pubsub: String? = null,
        val pulsar: PulsarNotify? = null,
    ) {
        companion object {
            fun default(): Notify {
                return Notify()
            }
        }
    }

    data class PulsarNotify(
        val url: String,
        val topic: String,
    )

    data class Auth(
        val gcp: AuthGcp? = null,
        val aws: AuthAws? = null,
    ) {
        companion object {
            fun default(): Auth {
                return Auth()
            }
        }
    }

    data class AuthGcp(
        val credentials: String,
    )

    data class AuthAws(
        val accessKey: String,
        val secretKey: String,
    )

    data class CompactionOptions(
        val acceptForks: Boolean = true,
        val compactRanges: Boolean = false,
    )
}
