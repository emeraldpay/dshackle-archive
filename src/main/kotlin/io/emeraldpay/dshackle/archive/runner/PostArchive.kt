package io.emeraldpay.dshackle.archive.runner

import java.nio.file.Path
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service

@Service
class PostArchive(
        @Autowired private val handlers: List<PostArchiveHandler>
) {

    companion object {
        private val log = LoggerFactory.getLogger(PostArchive::class.java)
    }

    fun handle(f: Path) {
        handlers.forEach { handler ->
            try {
                handler.handle(f)
            } catch (t: Throwable) {
                log.error("Failed to handle with ${handler.javaClass}", t)
            }
        }
    }

    fun close() {
        handlers.forEach(PostArchiveHandler::close)
    }
}