package com.cdc.poc.pubsub.subscriber.resource;

import com.cdc.poc.pubsub.subscriber.config.PersistResultWorker;
import com.cdc.poc.pubsub.subscriber.model.PocPubsubPerformanceHeader;
import com.cdc.poc.pubsub.subscriber.model.PubsubMessage;
import com.cdc.poc.pubsub.subscriber.model.PushRequest;
import com.cdc.poc.pubsub.subscriber.model.TestSubscriberResult;
import com.cdc.poc.pubsub.subscriber.repo.StressTestRepo;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import lombok.extern.slf4j.Slf4j;
import org.jboss.resteasy.reactive.RestResponse;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Path("/receive")
public class ReceiveResource {
    private static final AtomicLong messageCount = new AtomicLong(0);
    private static final AtomicLong lastLogTime = new AtomicLong(System.currentTimeMillis());
    private static final long LOG_INTERVAL_MS = 10000; // Log stats every 10 seconds

    @Inject
    ObjectMapper objectMapper;

    @Inject
    PersistResultWorker persistResultWorker;

    @POST
    public Uni<RestResponse<Boolean>> receive(Map<String, Object> pushRequestRaw) {
        long processingStartTime = System.nanoTime();
        try {
            Instant subscriberReceiveAt = Instant.now();
            PushRequest pushRequest = objectMapper.convertValue(pushRequestRaw, PushRequest.class);
            PubsubMessage pubsubMessage = pushRequest.message();
            Map<String, String> attributes = pubsubMessage.attributes();
            PocPubsubPerformanceHeader header = objectMapper.convertValue(attributes, PocPubsubPerformanceHeader.class);
            Instant publishTime = header.topicPublishTime() == null ? pubsubMessage.publishTime()
                    : header.topicPublishTime();
            long receiveLatencyMs = subscriberReceiveAt.toEpochMilli() - publishTime.toEpochMilli();
            long count = messageCount.incrementAndGet();
            long currentTime = System.currentTimeMillis();
            long lastLog = lastLogTime.get();
            if (currentTime - lastLog >= LOG_INTERVAL_MS) {
                if (lastLogTime.compareAndSet(lastLog, currentTime)) {
                    long interval = currentTime - lastLog;
                    log.info(
                            "Push subscriber throughput stats: subscriptionType={}, subscriptionId={}, messagesProcessed={}, intervalMs={}, avgMsgPerSec={}",
                            header.subscriptionType(), header.subscriptionId(), count, interval,
                                (count * 1000.0) / interval);
                    messageCount.set(0);
                }
            }
            log.debug(
                    "Processing push message: testId={}, messageId={}, subscriptionType={}, subscriptionId={}, publishTime={}, receiveTime={}, receiveLatencyMs={}, payloadSizeKb={}",
                    header.testId(), header.messageId(), header.subscriptionType(), header.subscriptionId(),
                    publishTime, subscriberReceiveAt, receiveLatencyMs, attributes.get("payloadSizeInKb"));
            persistResultWorker.getMessageHeaderQueue().add(new PersistResultWorker.PersistResultWorkerResult(header, subscriberReceiveAt, publishTime,""));
            return Uni.createFrom().item(RestResponse.ok(true));
        } catch (Exception e) {
            long processingDurationMs = (System.nanoTime() - processingStartTime) / 1_000_000;
            log.error("Failed to process push message after {}ms. Reason: {}, requestData: {}", processingDurationMs,
                    e.getMessage(), pushRequestRaw, e);
            return Uni.createFrom().item(RestResponse.ok(true));
        }
    }
}
