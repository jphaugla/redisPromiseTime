package com.howtodoinjava.streams.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.howtodoinjava.streams.model.OrderEvent;
import com.redis.testcontainers.RedisContainer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@SpringBootTest
@Testcontainers(disabledWithoutDocker = true)
class RedisStreamEventConsumerTest {

    @Autowired
    private RedisTemplate<String, String> redisTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    @SpyBean(name = "orderStreamListener")
    StreamListener<String, ObjectRecord<String, OrderEvent>> orderStreamListener;

    @Container
    /*@ServiceConnection*/
    private static final RedisContainer REDIS_CONTAINER =
            new RedisContainer(DockerImageName.parse("redis:5.0.3-alpine")).withExposedPorts(6379);

    @DynamicPropertySource
    private static void registerRedisProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.redis.host", REDIS_CONTAINER::getHost);
        registry.add("spring.data.redis.port", () -> REDIS_CONTAINER
                .getMappedPort(6379).toString());
    }

    @Test
    public void testOnMessage() throws Exception {

        OrderEvent orderEvent = OrderEvent.builder()
                .facilityId("1")
                .workOrderId("12")
                .promiseTime("2023-08-15 12:22:21")
                .verificationTime("2023-08-15 12:22:21")
                .queue("QV1")
                .build();

        String streamKey = "purchase-events";
        ObjectRecord<String, OrderEvent> record = StreamRecords.newRecord()
                .ofObject(orderEvent)
                .withStreamKey(streamKey);

        this.redisTemplate.opsForStream()
                .add(record);


        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);

        verify(orderStreamListener, times(1))
                .onMessage(isA(ObjectRecord.class));

        String fullKey = orderEvent.getFacilityId() + ":"
                + orderEvent.getWorkOrderId() + ":"
                + orderEvent.getPromiseTime();
        Map<Object, Object> orderHash = redisTemplate.opsForHash().entries(fullKey);

        OrderEvent receivedEvent = objectMapper.convertValue(
                orderHash,
                OrderEvent.class);

        assertEquals(receivedEvent.getFacilityId(), orderEvent.getFacilityId());
        assertEquals(receivedEvent.getWorkOrderId(), orderEvent.getWorkOrderId());
        assertEquals(receivedEvent.getPromiseTime(), orderEvent.getPromiseTime());
        assertEquals(receivedEvent.getVerificationTime(), orderEvent.getVerificationTime());

        // redisTemplate.expire(orderEvent.getPurchaseId(), Duration.ofSeconds(2));
    }

}