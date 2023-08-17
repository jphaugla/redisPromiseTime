package com.howtodoinjava.streams.producer.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.howtodoinjava.streams.model.OrderEvent;
import io.micrometer.common.util.StringUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.text.ParseException;
import java.util.Objects;

@Service
@Slf4j
public class OrderStreamProducer {

  @Autowired
  private RedisTemplate<String, String> redisTemplate;

  @Value("${stream.key:order-events}")
  private String streamKey;

  public RecordId produce(OrderEvent orderEvent) throws JsonProcessingException, ParseException {
    log.info("order details: {}", orderEvent);
    // convert promise time and verification time to epoch
    if (!StringUtils.isEmpty(orderEvent.getVerificationTime())) {
      long epochVerificationTime = OrderEvent.convertTime(orderEvent.getVerificationTime());
      orderEvent.setVerificationTime(String.valueOf(epochVerificationTime));
    }
    long epochPromiseTime = OrderEvent.convertTime(orderEvent.getPromiseTime());
    orderEvent.setPromiseTime(String.valueOf(epochPromiseTime));

    ObjectRecord<String, OrderEvent> record = StreamRecords.newRecord()
        .ofObject(orderEvent)
        .withStreamKey(streamKey);

    RecordId recordId = this.redisTemplate.opsForStream()
        .add(record);

    log.info("recordId: {}", recordId);

    if (Objects.isNull(recordId)) {
      log.info("error sending event: {}", orderEvent);
      return null;
    }

    return recordId;
  }
}
