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

@Slf4j
@ApplicationScoped
public class GCPPubsubRouteBuilder extends RouteBuilder {
    @ConfigProperty(name = "quarkus.google.cloud.project-id")
    String projectId;

    @ConfigProperty(name = "pubsub.subscription-id")
    String subscriptionId;

    @ConfigProperty(name = "pubsub.pull-options")
    String pullOptions;

    @Inject
    ObjectMapper objectMapper;

    @Inject
    StressTestRepo stressTestRepo;

    @Override
    @SuppressWarnings("unchecked")
    public void configure() {
        String consumerUri = MessageFormat.format("google-pubsub:{0}:{1}{2}", projectId, subscriptionId, pullOptions);
        log.info("Starting GCP pubsub component consumers with configs: {}", consumerUri);
        from(consumerUri).process(exchange -> {
            try {
                Instant subscriberReceiveAt = Instant.now();
                Map<String, String> attributes = (Map<String, String>) exchange.getIn().getHeader(GooglePubsubConstants.ATTRIBUTES, new HashMap<>(), Map.class);
                Timestamp timestamp = (Timestamp) exchange.getIn().getHeaders().get(GooglePubsubConstants.PUBLISH_TIME);
                Instant publishTime = Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
                PocPubsubPerformanceHeader header = objectMapper.convertValue(attributes, PocPubsubPerformanceHeader.class);
                stressTestRepo.updateTestResult(header.testId(), header.messageId(), header.topicArrivalTime(), header.topicPublishTime() == null ? publishTime : header.topicPublishTime());
                stressTestRepo.createTestSubscriberResult(new TestSubscriberResult(header.testId(), header.messageId(), header.subscriptionType(), header.subscriptionId(), header.subscriptionPublishTime() == null ? publishTime : header.subscriptionPublishTime(), header.subscriptionArrivalTime(), subscriberReceiveAt, pullOptions, Instant.now()));
            } catch (Exception e) {
                log.error("Failed to process message. Reason: {}", e.getMessage(), e);
            }
        });
    }
}
