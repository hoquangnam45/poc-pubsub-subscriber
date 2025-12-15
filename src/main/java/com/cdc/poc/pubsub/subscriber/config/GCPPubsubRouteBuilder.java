package com.cdc.poc.pubsub.subscriber.config;

import com.cdc.poc.pubsub.subscriber.model.PocPubsubPerformanceHeader;
import com.cdc.poc.pubsub.subscriber.model.TestSubscriberResult;
import com.cdc.poc.pubsub.subscriber.repo.StressTestRepo;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Timestamp;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.google.pubsub.GooglePubsubConstants;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.text.MessageFormat;
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

    @Inject
    ObjectMapper objectMapper;

    @Inject
    StressTestRepo stressTestRepo;

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

        String consumerUri = MessageFormat.format("google-pubsub:{0}:{1}{2}", projectId, subscriptionId, pullOptions);
        log.info("Starting GCP pubsub component consumers with configs: {}", consumerUri);
        from(consumerUri).process(exchange -> {
            long processingStartTime = System.nanoTime();
            try {
                Instant subscriberReceiveAt = Instant.now();
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
                                "Subscriber throughput stats: subscriptionType={}, subscriptionId={}, pullOptions={}, messagesProcessed={}, intervalMs={}, avgMsgPerSec={}",
                                header.subscriptionType(), header.subscriptionId(), pullOptions, count, interval,
                                (count * 1000.0) / interval);
                        messageCount.set(0);
                    }
                }
                log.debug(
                        "Processing message: testId={}, messageId={}, subscriptionType={}, subscriptionId={}, publishTime={}, receiveTime={}, receiveLatencyMs={}, payloadSizeKb={}",
                        header.testId(), header.messageId(), header.subscriptionType(), header.subscriptionId(),
                        publishTime, subscriberReceiveAt, receiveLatencyMs, attributes.get("payloadSizeInKb"));
                stressTestRepo.updateTopicResult(header.testId(), header.messageId(), header.topicArrivalTime(),
                        header.topicPublishTime() == null ? publishTime : header.topicPublishTime());
                stressTestRepo.createSubscriberResult(new TestSubscriberResult(header.testId(), header.messageId(),
                        header.subscriptionType(), header.subscriptionId(),
                        header.subscriptionPublishTime() == null ? publishTime : header.subscriptionPublishTime(),
                        header.subscriptionArrivalTime(), subscriberReceiveAt, pullOptions, Instant.now()));
            } catch (Exception e) {
                long processingDurationMs = (System.nanoTime() - processingStartTime) / 1_000_000;
                log.error("Failed to process message after {}ms. Reason: {}, exchangeHeaders: {}", processingDurationMs,
                        e.getMessage(), exchange.getIn().getHeaders(), e);
            }
        });
    }
}
