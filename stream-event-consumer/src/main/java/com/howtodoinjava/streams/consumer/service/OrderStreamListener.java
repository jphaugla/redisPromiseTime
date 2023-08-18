package com.howtodoinjava.streams.consumer.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.howtodoinjava.streams.model.OrderEvent;
import io.micrometer.common.util.StringUtils;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.annotation.Order;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import java.net.InetAddress;
import java.text.ParseException;
import java.util.Map;


@Slf4j
@Service
public class OrderStreamListener implements StreamListener<String, ObjectRecord<String, OrderEvent>> {

    @Value("${stream.key:order-events}")
    private String streamKey;
    @Inject
    OrderService orderService;
    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private RedisTemplate<String, String> redisTemplate;

    @Override
    @SneakyThrows

    public void onMessage(ObjectRecord<String, OrderEvent> record) {
        OrderEvent orderEvent = record.getValue();
        log.info(InetAddress.getLocalHost().getHostName() + " - consumed :" + orderEvent);
        orderService.processOrderEvent(orderEvent);
        redisTemplate.opsForStream().acknowledge(streamKey, record);
    }

}