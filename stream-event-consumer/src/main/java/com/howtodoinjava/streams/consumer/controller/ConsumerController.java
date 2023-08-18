package com.howtodoinjava.streams.consumer.controller;
import com.howtodoinjava.streams.consumer.service.OrderService;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;
import javax.inject.Inject;

import java.util.List;
import java.util.Map;

@Slf4j
@RestController

public class ConsumerController {
    @Inject
    OrderService orderService;

    @GetMapping("/status")
    public String status() {
        log.info("this is the status check");
        return "OK";
    }
    @GetMapping("/storeSuccess")
    public Map<Object, Object> getStoreSuccess (@RequestParam String store, @RequestParam String queue) {
        log.info("get store success " + store + " queue " + queue);
        return orderService.getFacilitySuccess(store, queue);
    }
    @GetMapping("/allStoreSuccess")
    public List<Map<Object, Object>> getAllFacilitySuccess() {
        log.info("get all store success");
        return orderService.getAllFacilitySuccess();
    }

    @GetMapping("/rebuildStoreSuccess")
    public String rebuildSuccessStats(@RequestParam String store, @RequestParam String queue) {
        orderService.rebuildSuccessStats(store, queue);
        return "Done";
    }


    @PostConstruct
    private void init() {
        log.info("init orderService");
    }
}
