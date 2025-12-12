package com.cdc.poc.pubsub.subscriber.model;

import java.time.Instant;
import java.util.UUID;

public record TestSubscriberResult(
        UUID testId,
        UUID messageId,
        String subscriptionType,
        String subscriptionId,
        Instant subscriptionPublishTime,
        Instant subscriptionArrivalTime,
        Instant subscriberArrivalTime,
        String pullOptions,
        Instant createdAt
) {
}
