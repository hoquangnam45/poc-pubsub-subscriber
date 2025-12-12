package com.cdc.poc.pubsub.subscriber.model;

public record PushRequest(PubsubMessage message, String subscription) {
}
