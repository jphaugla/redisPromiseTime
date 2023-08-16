package com.howtodoinjava.streams.producer;

import com.howtodoinjava.streams.model.OrderEvent;
import com.howtodoinjava.streams.producer.service.OrderStreamProducer;
import com.redis.testcontainers.RedisContainer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.junit.jupiter.api.Assertions.assertNotNull;

@SpringBootTest
@Testcontainers(disabledWithoutDocker = true)
class RedisStreamEventProducerTest {

  @Autowired
  private OrderStreamProducer eventProducer;

  @Container
  private static final RedisContainer REDIS_CONTAINER =
      new RedisContainer(DockerImageName.parse("redis:5.0.3-alpine")).withExposedPorts(6379);

  @DynamicPropertySource
  private static void registerRedisProperties(DynamicPropertyRegistry registry) {
    registry.add("spring.data.redis.host", REDIS_CONTAINER::getHost);
    registry.add("spring.data.redis.port", () -> REDIS_CONTAINER
        .getMappedPort(6379).toString());
  }

  @Test
  public void testProduce() throws Exception {

    OrderEvent orderEvent = OrderEvent.builder()
        .facilityId("02819")
        .workOrderId("1")
        .promiseTime("2023-05-06 12:30:30")
        .verificationTime("2023-05-07 12:30:30")
        .queue("QV1")
        .build();

    assertNotNull(eventProducer.produce(orderEvent));
  }
}