package com.cdc.poc.pubsub.subscriber.resource;

import com.cdc.poc.pubsub.subscriber.model.PocPubsubPerformanceHeader;
import com.cdc.poc.pubsub.subscriber.model.PubsubMessage;
import com.cdc.poc.pubsub.subscriber.model.PushRequest;
import com.cdc.poc.pubsub.subscriber.model.TestSubscriberResult;
import com.cdc.poc.pubsub.subscriber.repo.StressTestRepo;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import lombok.extern.slf4j.Slf4j;
import org.jboss.resteasy.reactive.RestResponse;

import java.time.Instant;
import java.util.Map;

@Slf4j
@Path("/receive")
public class ReceiveResource {
    @Inject
    StressTestRepo stressTestRepo;

    @Inject
    ObjectMapper objectMapper;

    @POST
    public Uni<RestResponse<Boolean>> receive(Map<String, Object> pushRequestRaw) {
        try {
            Instant subscriberReceiveAt = Instant.now();
            PushRequest pushRequest = objectMapper.convertValue(pushRequestRaw, PushRequest.class);
            PubsubMessage pubsubMessage = pushRequest.message();
            Map<String, String> attributes = pubsubMessage.attributes();
            PocPubsubPerformanceHeader header = objectMapper.convertValue(attributes, PocPubsubPerformanceHeader.class);
            stressTestRepo.updateTestResult(header.testId(), header.messageId(), header.topicArrivalTime(), header.topicPublishTime() == null ? pubsubMessage.publishTime() : header.topicPublishTime());
            stressTestRepo.createTestSubscriberResult(new TestSubscriberResult(header.testId(), header.messageId(), header.subscriptionType(), header.subscriptionId(), header.subscriptionPublishTime() == null ? pubsubMessage.publishTime() : header.subscriptionPublishTime(), header.subscriptionArrivalTime(), subscriberReceiveAt, "", Instant.now()));
            return Uni.createFrom().item(RestResponse.ok(true));
        } catch (Exception e) {
            log.error("Failed to process message. Reason: {}", e.getMessage(), e);
            return Uni.createFrom().item(RestResponse.ok(true));
        }
    }
}
