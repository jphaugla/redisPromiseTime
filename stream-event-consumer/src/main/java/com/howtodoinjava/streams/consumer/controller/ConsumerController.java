package com.howtodoinjava.streams.consumer.controller;

import com.howtodoinjava.streams.consumer.service.OrderService;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;
import javax.inject.Inject;

import java.text.ParseException;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

    @GetMapping("/recentStoreSales")
    public Set<String> recentStoreOrders(@RequestParam String store, @RequestParam String queue, @RequestParam String startTime, @RequestParam String endTime) throws ParseException {
        log.info("in recentStoreSales store " + store + " queue=" + queue + " start=" + startTime + " end=" + endTime);
        return orderService.recentStoreOrders(store, queue, startTime, endTime);
    }

    @GetMapping("/recentStoreQueueSales")
    public Set<String> recentStoreOrders(@RequestParam String storeQueue, @RequestParam String startTime, @RequestParam String endTime) throws ParseException {
        log.info("in recentStoreSales store " + storeQueue + " start=" + startTime + " end=" + endTime);
        return orderService.recentStoreOrders(storeQueue, startTime, endTime);
    }
    @GetMapping("/allStoreList")
    public Set<String> allStoreList() throws ParseException {
        return orderService.getAllStores();
    }

    @PostConstruct
    private void init() {
        log.info("init orderService");
    }
}
