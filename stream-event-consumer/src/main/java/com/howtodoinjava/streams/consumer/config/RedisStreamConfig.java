package com.howtodoinjava.streams.consumer.config;

import com.howtodoinjava.streams.consumer.service.OrderStreamListener;
import com.howtodoinjava.streams.model.OrderEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;
import org.springframework.data.redis.stream.Subscription;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class RedisStreamConfig {

  @Value("${stream.key:order-events}")
  private String streamKey;

  @Bean
  public StreamListener<String, ObjectRecord<String, OrderEvent>> orderStreamListener() {
    // handle message from stream
    return new OrderStreamListener();
  }

  @Bean
  public Subscription subscription(RedisConnectionFactory connectionFactory)
      throws UnknownHostException {

    createConsumerGroupIfNotExists(connectionFactory, streamKey, streamKey);
    StreamOffset<String> streamOffset = StreamOffset.create(streamKey, ReadOffset.lastConsumed());

    StreamMessageListenerContainer.StreamMessageListenerContainerOptions<String,
        ObjectRecord<String, OrderEvent>> options = StreamMessageListenerContainer
        .StreamMessageListenerContainerOptions
        .builder()
        .pollTimeout(Duration.ofMillis(100))
        .targetType(OrderEvent.class)
        .build();

    StreamMessageListenerContainer<String, ObjectRecord<String, OrderEvent>> container =
        StreamMessageListenerContainer
            .create(connectionFactory, options);

    Subscription subscription =
        container.receiveAutoAck(Consumer.from(streamKey, InetAddress.getLocalHost().getHostName()),
            streamOffset, orderStreamListener());

    /*
     * container.receiveAutoAck(....)
     * can also be used to auto acknowledge the messages just after being received.
     */

    container.start();
    return subscription;
  }

  private void createConsumerGroupIfNotExists(RedisConnectionFactory redisConnectionFactory,
      String streamKey, String groupName) {
    try {
      try {
        redisConnectionFactory.getConnection().streamCommands()
            .xGroupCreate(streamKey.getBytes(), streamKey, ReadOffset.from("0-0"), true);
      } catch (RedisSystemException exception) {
        log.warn(exception.getCause().getMessage());
      }
    } catch (RedisSystemException ex) {
      log.error(ex.getMessage());
    }
  }
}