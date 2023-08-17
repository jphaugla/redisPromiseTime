package com.howtodoinjava.streams.consumer.service;

import com.howtodoinjava.streams.model.OrderEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
@Service
public class OrderService {
    @Autowired
    private RedisTemplate<String, String> redisTemplate;
    public  Map<Object, Object> getFacilitySuccess(String facilityId, String queue) {
        log.info("in getFacilitySuccess " + facilityId);

        return(redisTemplate.opsForHash().
                entries(OrderEvent.getKeyNameStoreSuccess(facilityId, queue)));
    }

    public List<Map<Object, Object>> getAllFacilitySuccess() {
        Map<Object, Object> storeEntries;
        List<Map<Object, Object>> storeSuccessList = new ArrayList<Map<Object, Object>>();
        Set<String> stores = redisTemplate.opsForZSet().range(OrderEvent.getKeyNameAllStores(), 0, Double.valueOf(Double.POSITIVE_INFINITY).longValue());
        assert stores != null;
        for ( String storeQueueKey : stores) {
            String storeKey = OrderEvent.getKeyNameStoreSuccess(storeQueueKey);
            log.info("store key " + storeKey);
            storeEntries = redisTemplate.opsForHash().entries(storeKey);
            storeEntries.put("storeQueue",storeQueueKey);
            storeSuccessList.add(storeEntries);
        }
        return storeSuccessList;
    }
}
