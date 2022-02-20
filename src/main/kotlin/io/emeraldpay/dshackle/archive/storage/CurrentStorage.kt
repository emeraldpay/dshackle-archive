package io.emeraldpay.dshackle.archive.storage

import java.io.IOException
import java.nio.file.Path
import java.util.*
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write
import kotlin.io.path.absolutePathString
import org.slf4j.LoggerFactory

class CurrentStorage(
        private val limit: Int = 32
) : AutoCloseable {

    companion object {
        private val log = LoggerFactory.getLogger(CurrentStorage::class.java)
    }

    private val current = LinkedList<Ref<*>>()
    private val lock = ReentrantReadWriteLock()

    fun <T : AutoCloseable> get(path: Path, factory: () -> T): T {
        lock.read {
            val x = current.find {
                it.path == path
            }
            if (x != null) {
                return x.storage as T
            }
        }
        lock.write {
            val x = current.find {
                it.path == path
            }
            if (x != null) {
                return x.storage as T
            }
            val y = Ref(path, factory.invoke())
            current.addLast(y)
            if (current.size > limit) {
                try {
                    current.removeFirst().also { it.storage.close() }
                } catch (t: IOException) {
                    log.warn("Failed to close [${path.absolutePathString()}]. ${t.javaClass}: ${t.message}")
                }
            }
            return y.storage
        }
    }

    fun remove(path: Path) {
        lock.write {
            current.removeIf {
                it.path == path
            }
        }
    }

    data class Ref<T : AutoCloseable>(
            val path: Path,
            val storage: T
    )

    override fun close() {
        lock.write {
            current.forEach {
                it.storage.close()
            }
            current.clear()
        }
    }

}