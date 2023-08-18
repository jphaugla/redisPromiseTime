package com.howtodoinjava.streams.consumer.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.howtodoinjava.streams.model.OrderEvent;
import io.micrometer.common.util.StringUtils;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
@Service
public class OrderService {
    @Autowired
    private RedisTemplate<String, String> redisTemplate;
    private static final String STATUS_OK = "SUCCESS";
    private static final String STATUS_LATE = "LATE";
    // return the success metrics for a facility:queue
    public  Map<Object, Object> getFacilitySuccess(String facilityId, String queue) {
        log.info("in getFacilitySuccess " + facilityId);

        return(redisTemplate.opsForHash().
                entries(OrderEvent.getKeyNameStoreSuccess(facilityId, queue)));
    }
    // get all stores in a set
    public Set<String> getAllStores() {
        return redisTemplate.opsForZSet().range(OrderEvent.getKeyNameAllStores(), 0, Double.valueOf(Double.POSITIVE_INFINITY).longValue());
    }
    //  return the success metrics for all the facilities
    public List<Map<Object, Object>> getAllFacilitySuccess() {
        Map<Object, Object> storeEntries;
        List<Map<Object, Object>> storeSuccessList = new ArrayList<Map<Object, Object>>();
        Set<String> stores = getAllStores();
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
    @SneakyThrows
    public void processOrderEvent(OrderEvent orderEvent) throws ParseException {
        if ( StringUtils.isEmpty(orderEvent.getVerificationTime())) {
            //  since no verification time, this is the initial record
            redisTemplate.opsForZSet().add(OrderEvent.getKeyNameUnfilled(orderEvent.getFacilityId(),orderEvent.getQueue()),
                    orderEvent.getWorkOrderId(), Long.parseLong(orderEvent.getPromiseTime()));
        } else {
            //  this is the final record with the verification time
            //  set Success Flag in orderEvent
            orderEvent.setSuccess(getSuccess(orderEvent.getPromiseTime(), orderEvent.getVerificationTime()));
            // completed/Filled work orders per store zset with hash key as member value and promise time as the score
            redisTemplate.opsForZSet().add(OrderEvent.getKeyNameFullPromise(orderEvent.getFacilityId(),orderEvent.getQueue()),
                    getAllRecords(orderEvent), Long.parseLong(orderEvent.getPromiseTime()));
            // completed/Filled work orders per store zset with hash key as member value and verification time as the score
            redisTemplate.opsForZSet().add(OrderEvent.getKeyNameFullVerif(orderEvent.getFacilityId(),orderEvent.getQueue()),
                    getAllRecords(orderEvent), Long.parseLong(orderEvent.getVerificationTime()));
            //  set success for the facility in the sorted set
            setFacilitySuccess(orderEvent.getFacilityId(), orderEvent.getQueue(), orderEvent.getSuccess());
            //  remove the record from the unfilled zset for the facility
            redisTemplate.opsForZSet().remove(OrderEvent.getKeyNameUnfilled(orderEvent.getFacilityId(),orderEvent.getQueue()),
                    orderEvent.getWorkOrderId());
            //  maintain list of all stores seen with last time seen using verification time
            redisTemplate.opsForZSet().add(OrderEvent.getKeyNameAllStores(),orderEvent.getFacilityId() + ":"
                    + orderEvent.getQueue(),Long.parseLong(orderEvent.getVerificationTime()));

        }
        //  write to the hash either way.  This hash is main record of all the transactions
        ObjectMapper mapObject = new ObjectMapper();
        Map < Object, Object > orderHash = mapObject.convertValue(orderEvent, Map.class);
        redisTemplate.opsForHash().putAll(OrderEvent.getKeyNameOrderPromise(orderEvent.getFacilityId(),orderEvent.getWorkOrderId(),orderEvent.getPromiseTime()), orderHash);

    }
    private String getSuccess(String promiseTime, String verificationTime) throws ParseException {
        String returnVal;
        if (Long.parseLong(promiseTime) <= Long.parseLong(verificationTime)) {
            returnVal = STATUS_OK;
        } else {
            returnVal = STATUS_LATE;
        }
        return (returnVal);
    }
    public void setFacilitySuccess(String inFacilityId, String inQueue, String inSuccess) {
        //  if success, add counter for the store in success zset with store as member value and success count as score

        redisTemplate.opsForHash().increment(OrderEvent.getKeyNameStoreSuccess(inFacilityId, inQueue),
                    inSuccess, 1);
    }
    public void rebuildSuccessStats(String inFacilityId, String inQueue){

        Set<String> concatValueSet = redisTemplate.opsForZSet().range(OrderEvent.getKeyNameFullVerif(inFacilityId, inQueue),
                0, Double.valueOf(Double.POSITIVE_INFINITY).longValue());
        assert concatValueSet != null;
        String successValue = null;
        for ( String concatValue : concatValueSet) {
            if(concatValue.contains(":" + STATUS_OK)) {
                successValue = STATUS_OK;
            } else {
                successValue = STATUS_LATE;
            }
            log.info("full Value " + concatValue);
            setFacilitySuccess(inFacilityId, inQueue, successValue);
        }
    }
    private static String getAllRecords(OrderEvent inOrder) {
        String returnVal = inOrder.getFacilityId() + ":" + inOrder.getWorkOrderId() + ":" + inOrder.getPromiseTime()
                + ":" + inOrder.getQueue() + ":" + inOrder.getVerificationTime()
                + ":" + inOrder.getSuccess();
        return (returnVal);
    }


    public Set<String> recentStoreOrders(String inFacilityId, String inQueue, String startTime, String endTime) throws ParseException {
        long startMillis = Long.parseLong(startTime);
        long endMillis = Long.parseLong(endTime);
        log.info("in recentStoreOrders store=" + inFacilityId + " queue=" + inQueue + " start=" + startTime + " end=" + endTime);
        Set<String> stores = redisTemplate.opsForZSet().rangeByScore(OrderEvent.getKeyNameFullVerif(inFacilityId, inQueue), startMillis, endMillis);
        return stores;
    }

    public Set<String> recentStoreOrders(String storeQueue, String startTime, String endTime) {
        long startMillis = Long.parseLong(startTime);
        long endMillis = Long.parseLong(endTime);
        log.info("in recentStoreOrders store=" + storeQueue + " start=" + startTime + " end=" + endTime);
        Set<String> stores = redisTemplate.opsForZSet().rangeByScore(OrderEvent.getKeyNameFullVerif(storeQueue), startMillis, endMillis);
        return stores;
    }
}
