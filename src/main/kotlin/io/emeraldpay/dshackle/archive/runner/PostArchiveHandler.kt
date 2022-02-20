package io.emeraldpay.dshackle.archive.runner

import java.nio.file.Path

interface PostArchiveHandler {

    fun handle(f: Path)
    fun close()

}