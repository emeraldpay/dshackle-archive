package io.emeraldpay.dshackle.archive.notify

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.api.gax.core.CredentialsProvider
import com.google.api.gax.core.NoCredentialsProvider
import com.google.api.gax.grpc.GrpcTransportChannel
import com.google.api.gax.rpc.FixedTransportChannelProvider
import com.google.api.gax.rpc.TransportChannelProvider
import com.google.cloud.pubsub.v1.SubscriptionAdminClient
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings
import com.google.cloud.pubsub.v1.TopicAdminClient
import com.google.cloud.pubsub.v1.TopicAdminSettings
import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub
import com.google.cloud.pubsub.v1.stub.SubscriberStub
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings
import com.google.pubsub.v1.ProjectSubscriptionName
import com.google.pubsub.v1.ProjectTopicName
import com.google.pubsub.v1.PullRequest
import com.google.pubsub.v1.PullResponse
import com.google.pubsub.v1.PushConfig
import com.google.pubsub.v1.SubscriptionName
import com.google.pubsub.v1.TopicName
import io.emeraldpay.dshackle.archive.config.GoogleAuthProvider
import io.emeraldpay.dshackle.archive.config.RunConfig
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import org.testcontainers.containers.PubSubEmulatorContainer
import org.testcontainers.utility.DockerImageName
import spock.lang.Specification

class PubsubNotifierIntegrationSpec extends Specification {

    def projectId = "emerald-testing"
    def topicId = "dshackle-archive-notify"
    def subscriptionId = "dshackle-archive-notify-sub"

    TransportChannelProvider channelProvider
    CredentialsProvider credentialsProvider
    SubscriberStubSettings subscriberStubSettings

    PubsubNotifier notifier

    def setup() {
        PubSubEmulatorContainer emulator = new PubSubEmulatorContainer(
                DockerImageName.parse("gcr.io/google.com/cloudsdktool/cloud-sdk:367.0.0-emulators")
        )
        emulator.start()
        String hostport = emulator.getEmulatorEndpoint();
        ManagedChannel channel = ManagedChannelBuilder.forTarget(hostport).usePlaintext().build();
        channelProvider = FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel));
        credentialsProvider = NoCredentialsProvider.create()

        TopicAdminSettings topicAdminSettings = TopicAdminSettings.newBuilder()
                .setTransportChannelProvider(channelProvider)
                .setCredentialsProvider(credentialsProvider)
                .build();

        TopicName topicName = TopicName.of(projectId, topicId);
        try (TopicAdminClient topicAdminClient = TopicAdminClient.create(topicAdminSettings)) {
            topicAdminClient.createTopic(topicName);
        }

        SubscriptionAdminSettings subscriptionAdminSettings = SubscriptionAdminSettings.newBuilder()
                .setTransportChannelProvider(channelProvider)
                .setCredentialsProvider(credentialsProvider)
                .build();
        SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient.create(subscriptionAdminSettings);
        SubscriptionName subscriptionName = SubscriptionName.of(projectId, subscriptionId);
        subscriptionAdminClient.createSubscription(subscriptionName, topicName, PushConfig.getDefaultInstance(), 10);

        subscriberStubSettings = SubscriberStubSettings.newBuilder()
                        .setTransportChannelProvider(channelProvider)
                        .setCredentialsProvider(credentialsProvider)
                        .build();

        notifier = new PubsubNotifier(
                ProjectTopicName.format(projectId, topicId),
                new ObjectMapper()
        )
        notifier.channelProvider = channelProvider
        notifier.credentialsProvider = new GoogleAuthProvider.Delegated(RunConfig.default(), credentialsProvider)
    }

    def "Can send a message"() {
        setup:
        notifier.start()
        SubscriberStub subscriber = GrpcSubscriberStub.create(subscriberStubSettings)

        when:
        notifier.onCreated('{"foo":"bar"}').block()
        PullRequest pullRequest = PullRequest.newBuilder()
                .setMaxMessages(1)
                .setSubscription(ProjectSubscriptionName.format(projectId, subscriptionId))
                .build();
        PullResponse pullResponse = subscriber.pullCallable().call(pullRequest);

        then:
        pullResponse.getReceivedMessagesList().size() == 1
        with(pullResponse.getReceivedMessages(0)) {
            it.message.data.toStringUtf8() == '{"foo":"bar"}'
        }
    }

}
