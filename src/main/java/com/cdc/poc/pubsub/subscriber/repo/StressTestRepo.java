package com.cdc.poc.pubsub.subscriber.repo;

import com.cdc.poc.pubsub.subscriber.model.TestSubscriberResult;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.time.Instant;
import java.util.UUID;

@Mapper
public interface StressTestRepo {
    void updateTestResult(@Param("testId") UUID testId, @Param("messageId") UUID messageId, @Param("topicArrivalTime") Instant topicArrivalTime, @Param("topicPublishTime") Instant topicPublishTime);
    void createTestSubscriberResult(TestSubscriberResult result);
}
