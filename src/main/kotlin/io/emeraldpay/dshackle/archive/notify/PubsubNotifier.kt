package io.emeraldpay.dshackle.archive.notify

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.api.core.ApiFuture
import com.google.api.core.ApiFutureCallback
import com.google.api.core.ApiFutures
import com.google.api.gax.rpc.TransportChannelProvider
import com.google.cloud.pubsub.v1.Publisher
import com.google.protobuf.ByteString
import com.google.pubsub.v1.PubsubMessage
import io.emeraldpay.dshackle.archive.config.GoogleAuthProvider
import io.emeraldpay.dshackle.archive.model.ProcessedException
import org.slf4j.LoggerFactory
import org.springframework.context.Lifecycle
import reactor.core.publisher.Mono
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

class PubsubNotifier(
    private val topic: String,
    objectMapper: ObjectMapper,
) : Notifier.JsonBased(objectMapper), Lifecycle {

    companion object {
        private val log = LoggerFactory.getLogger(PubsubNotifier::class.java)
    }

    var credentialsProvider: GoogleAuthProvider? = null
    var channelProvider: TransportChannelProvider? = null

    private val executor = Executors.newSingleThreadExecutor()
    private var publisher: AtomicReference<Publisher?> = AtomicReference(null)

    override fun onCreated(archiveJson: String): Mono<Void> {
        return Mono.fromCompletionStage(send(archiveJson))
            .then()
    }

    private fun send(json: String): CompletableFuture<String> {
        val publisher = publisher.get() ?: return CompletableFuture.failedFuture(IllegalStateException("Pubsub Publisher is not started"))
        val future = CompletableFuture<String>()

        val data: ByteString = ByteString.copyFromUtf8(json)
        val pubsubMessage = PubsubMessage.newBuilder()
            .setData(data)
            .build()
        val messageIdFuture: ApiFuture<String> = publisher.publish(pubsubMessage)
        ApiFutures.addCallback(
            messageIdFuture,
            object : ApiFutureCallback<String> {
                override fun onSuccess(messageId: String) {
                    future.complete(messageId)
                }

                override fun onFailure(t: Throwable) {
                    if (t is com.google.api.gax.rpc.PermissionDeniedException) {
                        log.warn("Permission Denied to publish to topic $topic from user ${credentialsProvider?.username}")
                        future.completeExceptionally(ProcessedException(t))
                    } else {
                        future.completeExceptionally(t)
                    }
                }
            },
            executor,
        )

        return future
    }

    override fun start() {
        log.info("Connecting to PubSub topic: $topic")
        publisher.updateAndGet { current ->
            if (current != null) {
                current.shutdown()
                current.awaitTermination(1, TimeUnit.MINUTES)
            }
            Publisher.newBuilder(topic)
                .let { builder -> credentialsProvider?.let { builder.setCredentialsProvider(it) } ?: builder }
                .let { builder -> channelProvider?.let { builder.setChannelProvider(it) } ?: builder }
                .build()
        }
        log.debug("Connected to PubSub topic: $topic")
    }

    override fun stop() {
        publisher.updateAndGet { current ->
            if (current != null) {
                current.shutdown()
                current.awaitTermination(1, TimeUnit.MINUTES)
            }
            null
        }
    }

    override fun isRunning(): Boolean {
        return publisher.get() != null
    }
}
