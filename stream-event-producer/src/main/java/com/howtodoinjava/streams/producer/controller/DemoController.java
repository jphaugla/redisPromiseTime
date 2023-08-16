package com.howtodoinjava.streams.producer.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.howtodoinjava.streams.model.OrderEvent;
import com.howtodoinjava.streams.producer.service.OrderStreamProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class DemoController {

    @Autowired
    private OrderStreamProducer eventProducer;

    @PostMapping("/produce")
    public RecordId produceEvent(@RequestBody OrderEvent orderEvent) throws JsonProcessingException {
       return eventProducer.produce(orderEvent);
    }
}
