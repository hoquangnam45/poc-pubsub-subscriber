package com.cdc.poc.pubsub.subscriber.model;

import java.time.Instant;
import java.util.Map;

public record PubsubMessage(
     String data,
     Map<String, String> attributes,
     String messageId,
     Instant publishTime
) {
}
