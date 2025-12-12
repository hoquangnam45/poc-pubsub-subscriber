package com.cdc.poc.pubsub.subscriber.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record PocPubsubPerformanceHeader(
        @JsonProperty("topic_arrival_time") Instant topicArrivalTime,
        @JsonProperty("topic_id") String topicId,
        @JsonProperty("topic_message_id") String topicMessageId,
        @JsonProperty("topic_publish_time") Instant topicPublishTime,
        @JsonProperty("subscription_id") String subscriptionId,
        @JsonProperty("subscription_message_id") String subscriptionMessageId,
        @JsonProperty("subscription_publish_time") Instant subscriptionPublishTime,
        @JsonProperty("subscription_arrival_time") Instant subscriptionArrivalTime,
        @JsonProperty("subscription_type") String subscriptionType,
        @JsonProperty("testId") UUID testId,
        @JsonProperty("messageId") UUID messageId,
        @JsonProperty("payloadSizeInKb") BigDecimal payloadSizeInKb,
        @JsonProperty("creationTimeStamp") Instant creationTimestamp
) {
}
