package io.emeraldpay.dshackle.archive.storage

open class BucketPath(
    private val bucketPath: String,
) {

    fun fullPathFor(path: String): String {
        return listOf(bucketPath, path)
            .filter { it != null && it.isNotEmpty() }
            .joinToString("/")
            .replace(Regex("//+"), "/")
    }

    fun forBlockchain(blockchainDir: String): BlockchainPath {
        return BlockchainPath(blockchainDir, this)
    }

    class BlockchainPath(
        private val blockchainDir: String,
        private val bucketPath: BucketPath,
    ) {

        fun fullPathFor(path: String): String {
            return bucketPath.fullPathFor(
                listOf(blockchainDir, path).joinToString("/"),
            )
        }
    }
}
