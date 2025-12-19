package com.cdc.poc.pubsub.subscriber.config;

import com.cdc.poc.pubsub.subscriber.model.PocPubsubPerformanceHeader;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.core.InstantiatingExecutorProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.stub.PublisherStubSettings;
import com.google.protobuf.Timestamp;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.google.pubsub.GooglePubsubComponent;
import org.apache.camel.component.google.pubsub.GooglePubsubConstants;
import org.apache.camel.component.google.pubsub.GooglePubsubEndpoint;
import org.apache.camel.support.ResourceHelper;
import org.apache.camel.util.ObjectHelper;
import org.apache.camel.util.StringHelper;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.io.IOException;
import java.text.MessageFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@ApplicationScoped
public class GCPPubsubRouteBuilder extends RouteBuilder {
    @ConfigProperty(name = "quarkus.google.cloud.project-id")
    String projectId;

    @ConfigProperty(name = "pubsub.subscription-id")
    Optional<String> subscriptionIdOpt;

    @ConfigProperty(name = "pubsub.pull-options")
    Optional<String> pullOptionsOpt;

    @ConfigProperty(name = "pubsub.subscriber.parallel-pull-count")
    Optional<Integer> parallelPullCountOpt;

    @ConfigProperty(name = "pubsub.subscriber.executor-thread-count")
    Optional<Integer> executorThreadCountOpt;

    @Inject
    ObjectMapper objectMapper;

    @Inject
    PersistResultWorker persistResultWorker;

    @Inject
    CamelContext camelContext;

    private static final AtomicLong messageCount = new AtomicLong(0);
    private static final AtomicLong lastLogTime = new AtomicLong(System.currentTimeMillis());
    private static final long LOG_INTERVAL_MS = 10000; // Log stats every 10 seconds

    @Override
    @SuppressWarnings("unchecked")
    public void configure() {
        // Skip configuring Camel routes if subscription is disabled (e.g., for
        // PUSH-only deployments)
        if (subscriptionIdOpt.isEmpty()) {
            log.info("Skipping GCP Pub/Sub pull consumer routes - subscription disabled");
            return;
        }
        String subscriptionId = subscriptionIdOpt.get();
        String pullOptions = pullOptionsOpt.orElse("");

        boolean isCustomComponentNeeded = parallelPullCountOpt.isPresent() || executorThreadCountOpt.isPresent();
        String consumerUri;
        if (isCustomComponentNeeded) {
            camelContext.addComponent("custom-google-pubsub", new CustomPubsubComponent(parallelPullCountOpt.orElse(null), executorThreadCountOpt.orElse(null)));
            consumerUri = MessageFormat.format("custom-google-pubsub:{0}:{1}{2}", projectId, subscriptionId, pullOptions);
        } else {
            consumerUri = MessageFormat.format("google-pubsub:{0}:{1}{2}", projectId, subscriptionId, pullOptions);
        }
        log.info("Starting GCP pubsub component consumers with configs: {}", consumerUri);
        from(consumerUri).process(exchange -> {
            Instant subscriberReceiveAt = Instant.now();
            try {
                Map<String, String> attributes = (Map<String, String>) exchange.getIn()
                        .getHeader(GooglePubsubConstants.ATTRIBUTES, new HashMap<>(), Map.class);
                Timestamp timestamp = (Timestamp) exchange.getIn().getHeaders().get(GooglePubsubConstants.PUBLISH_TIME);
                Instant publishTime = Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
                PocPubsubPerformanceHeader header = objectMapper.convertValue(attributes,
                        PocPubsubPerformanceHeader.class);
                long receiveLatencyMs = subscriberReceiveAt.toEpochMilli() - publishTime.toEpochMilli();
                long count = messageCount.incrementAndGet();
                long currentTime = System.currentTimeMillis();
                long lastLog = lastLogTime.get();
                if (currentTime - lastLog >= LOG_INTERVAL_MS) {
                    if (lastLogTime.compareAndSet(lastLog, currentTime)) {
                        long interval = currentTime - lastLog;
                        log.info(
                                "Subscriber throughput stats: subscriptionType={}, subscriptionId={}, topicId={}, pullOptions={}, messagesProcessed={}, intervalMs={}, avgMsgPerSec={}",
                                header.subscriptionType(), header.subscriptionId(), header.topicId(), pullOptions, count, interval,
                                (count * 1000.0) / interval);
                        messageCount.set(0);
                    }
                }
                log.debug(
                        "Processing message: testId={}, messageId={}, subscriptionType={}, subscriptionId={}, publishTime={}, receiveTime={}, receiveLatencyMs={}, payloadSizeKb={}",
                        header.testId(), header.messageId(), header.subscriptionType(), header.subscriptionId(),
                        publishTime, subscriberReceiveAt, receiveLatencyMs, attributes.get("payloadSizeInKb"));
                persistResultWorker.getMessageHeaderQueue().add(new PersistResultWorker.PersistResultWorkerResult(header, subscriberReceiveAt, publishTime, pullOptions));
            } catch (Exception e) {
                long processingDurationMs = Duration.between(subscriberReceiveAt, Instant.now()).toMillis();
                log.error("Failed to process message after {}ms. Reason: {}, exchangeHeaders: {}", processingDurationMs,
                        e.getMessage(), exchange.getIn().getHeaders(), e);
            }
        });
    }

    @RequiredArgsConstructor
    public static class CustomPubsubComponent extends GooglePubsubComponent {
        private final Integer parallelPullCount;
        private final Integer executorThreadCount;

        @Override
        public Subscriber getSubscriber(String subscriptionName, MessageReceiver messageReceiver, GooglePubsubEndpoint googlePubsubEndpoint) throws IOException {
            Subscriber.Builder builder = Subscriber.newBuilder(subscriptionName, messageReceiver);
            if (StringHelper.trimToNull(getEndpoint()) != null) {
                ManagedChannel channel = ManagedChannelBuilder.forTarget(getEndpoint()).usePlaintext().build();
                TransportChannelProvider channelProvider = FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel));
                builder.setChannelProvider(channelProvider);
            }

            builder.setCredentialsProvider(this.getCredentialsProvider(googlePubsubEndpoint));
            builder.setMaxAckExtensionPeriod(org.threeten.bp.Duration.ofSeconds((long)googlePubsubEndpoint.getMaxAckExtensionPeriod()));

            // NOTE: Custom logic to change the parallelPullCount and executorThreadCount
            if (parallelPullCount != null) {
                builder.setParallelPullCount(parallelPullCount);
            }
            if (executorThreadCount != null) {
                builder.setExecutorProvider(InstantiatingExecutorProvider.newBuilder().setExecutorThreadCount(executorThreadCount).build());
            }
            return builder.build();
        }

        private CredentialsProvider getCredentialsProvider(GooglePubsubEndpoint endpoint) throws IOException {
            CredentialsProvider credentialsProvider;
            if (endpoint.isAuthenticate()) {
                credentialsProvider = FixedCredentialsProvider.create(ObjectHelper.isEmpty(endpoint.getServiceAccountKey()) ? GoogleCredentials.getApplicationDefault() : ServiceAccountCredentials.fromStream(ResourceHelper.resolveMandatoryResourceAsInputStream(this.getCamelContext(), endpoint.getServiceAccountKey())).createScoped(PublisherStubSettings.getDefaultServiceScopes()));
            } else {
                credentialsProvider = NoCredentialsProvider.create();
            }

            return credentialsProvider;
        }
    }
}
