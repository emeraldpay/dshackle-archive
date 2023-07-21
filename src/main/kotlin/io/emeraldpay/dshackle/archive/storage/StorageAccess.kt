package io.emeraldpay.dshackle.archive.storage

import io.emeraldpay.dshackle.archive.model.Chunk
import org.apache.avro.file.SeekableInput
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.io.OutputStream
import java.nio.file.Path

interface StorageAccess {

    fun listArchiveLevel0(height: Long): Flux<String>
    fun deleteArchives(files: List<String>): Mono<Void>

    /**
     * Get a _full_ URI to the file under current storage, to access by an external service.
     */
    fun getURI(file: String): String

    fun exists(path: String): Boolean

    /**
     * Creates a new writer to put data to the storage at path
     */
    fun createWriter(path: String): OutputStream

    /**
     * Read an existing file in the archive
     */
    fun createReader(path: String): SeekableInput
    fun createReader(fullPath: Path): SeekableInput

    fun getDirBlockSizeL1(): Long

    fun listArchive(height: Long): Flux<String> {
        val firstLevel0Pos = height / getDirBlockSizeL1()
        return listArchiveLevel0(height)
            .concatWith(nextLevel0(firstLevel0Pos.toInt() + 1))
    }

    /**
     * Recursively checks all heights at level0 starting from the specified pos. Stops when a Level 0 dir has no elements.
     *
     * Because the target archive keeps files in different directories (as Level 0)
     * which are also used to narrow the query we need to iterate all Level 0 dirs after the
     * initial height.
     *
     * @param index of level 0 dir; note it's not the height
     * @return elements at each as Level 0 after the specified, until the dir doesn't exist/empty
     */
    fun nextLevel0(pos: Int): Flux<String> {
        val items = Mono.fromCallable {
            getDirBlockSizeL1() * pos
        }.flatMapMany {
            listArchiveLevel0(it)
        }
        return items.switchOnFirst { t, u ->
            if (t.hasValue()) {
                // if we have got any file in the dir then also check the next level 0 dir for items
                u.concatWith(nextLevel0(pos + 1))
            } else {
                // if dir is empty just stop at this point and return nothing
                Flux.empty()
            }
        }
    }

    fun inputSources(patterns: List<String>, range: Chunk): InputSources

    data class InputSources(
        val transactions: Flux<Path>,
        val blocks: Flux<Path>,
    )
}
