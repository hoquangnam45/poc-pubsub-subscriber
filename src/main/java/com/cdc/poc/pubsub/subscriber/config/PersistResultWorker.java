package com.cdc.poc.pubsub.subscriber.config;

import com.cdc.poc.pubsub.subscriber.model.PocPubsubPerformanceHeader;
import com.cdc.poc.pubsub.subscriber.model.TestSubscriberResult;
import com.cdc.poc.pubsub.subscriber.repo.StressTestRepo;
import io.quarkus.runtime.Startup;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.time.Instant;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

@Getter
@Slf4j
@ApplicationScoped
public class PersistResultWorker {
    private final LinkedBlockingQueue<PersistResultWorkerResult> messageHeaderQueue = new LinkedBlockingQueue<>();

    @ConfigProperty(name = "workers.persist-result.size", defaultValue = "10")
    Integer workerSize;

    @Inject
    StressTestRepo stressTestRepo;

    @Startup
    void onStart() {
        ExecutorService executorService = Executors.newFixedThreadPool(workerSize);
        executorService.submit(() -> {
            while (true) {
                var res = messageHeaderQueue.take();
                var header = res.header();
                try {
                    Instant topicPublishTime = header.topicPublishTime() == null ? res.topicPublishTime()
                            : header.topicPublishTime();
                    stressTestRepo.insertTopicResult(header.testId(), header.messageId(), header.topicArrivalTime(),
                            topicPublishTime);
                    stressTestRepo.createSubscriberResult(new TestSubscriberResult(header.testId(), header.messageId(),
                            header.subscriptionType(), header.subscriptionId(),
                            header.subscriptionPublishTime() == null ? res.topicPublishTime()
                                    : header.subscriptionPublishTime(),
                            header.subscriptionArrivalTime(), res.subscriberReceiveAt(), res.pullOptions(), Instant.now()));
                } catch (Exception e) {
                    log.error("Error persisting subscriber result. testId={}, messageId={}, subscriptionId={}, pullOptions={}. Reason: {}", header.testId(), header.messageId(), header.subscriptionId(), res.pullOptions(), e.getMessage(), e);
                }
            }
        });
    }

    public record PersistResultWorkerResult(PocPubsubPerformanceHeader header, Instant subscriberReceiveAt,
                                            Instant topicPublishTime, String pullOptions) {
    }
}
