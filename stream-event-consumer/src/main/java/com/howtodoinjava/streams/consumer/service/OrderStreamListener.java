package com.howtodoinjava.streams.consumer.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.howtodoinjava.streams.model.OrderEvent;
import io.micrometer.common.util.StringUtils;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.stereotype.Service;

import java.net.InetAddress;
import java.text.ParseException;
import java.util.Map;


@Slf4j
@Service
public class OrderStreamListener implements StreamListener<String, ObjectRecord<String, OrderEvent>> {

    @Value("${stream.key:order-events}")
    private String streamKey;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private RedisTemplate<String, String> redisTemplate;
    private static final String STATUS_OK = "OK";
    private static final String STATUS_LATE = "LATE";
    @Override
    @SneakyThrows

    public void onMessage(ObjectRecord<String, OrderEvent> record) {
        OrderEvent orderEvent = record.getValue();
        log.info(InetAddress.getLocalHost().getHostName() + " - consumed :" + orderEvent);

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
                    OrderEvent.getKeyNameOrderPromise(orderEvent.getFacilityId(),orderEvent.getWorkOrderId(),orderEvent.getPromiseTime()), Long.parseLong(orderEvent.getPromiseTime()));
            // completed/Filled work orders per store zset with hash key as member value and verification time as the score
            redisTemplate.opsForZSet().add(OrderEvent.getKeyNameFullVerif(orderEvent.getFacilityId(),orderEvent.getQueue()),
                    OrderEvent.getKeyNameOrderPromise(orderEvent.getFacilityId(),orderEvent.getWorkOrderId(),orderEvent.getPromiseTime()), Long.parseLong(orderEvent.getVerificationTime()));
            //  if success, add counter for the store in success zset with store as member value and success count as score
            if(orderEvent.getSuccess().equals(STATUS_OK)) {
                //  score
                // redisTemplate.opsForZSet().incrementScore(getStoreSuccess(orderEvent),
                //        orderEvent.getFacilityId(),1);
                redisTemplate.opsForHash().increment(OrderEvent.getKeyNameStoreSuccess(orderEvent.getFacilityId(), orderEvent.getQueue()),
                        "success", 1);
            } else {
                //  if failure, add counter for the store in failure zset with store as member value and success count as score
                // redisTemplate.opsForZSet().incrementScore(getStoreFailure(orderEvent),
                 //       orderEvent.getFacilityId(),1);
                redisTemplate.opsForHash().increment(OrderEvent.getKeyNameStoreSuccess(orderEvent.getFacilityId(), orderEvent.getQueue()),
                        "failure", 1);

            }
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
        redisTemplate.opsForStream().acknowledge(streamKey, record);
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
    public static String getAllRecords(OrderEvent inOrder) {
        String returnVal = inOrder.getFacilityId() + ":" + inOrder.getWorkOrderId() + ":"
                + inOrder.getQueue() + ":" + inOrder.getPromiseTime() + ":" + inOrder.getVerificationTime()
                + ":" + inOrder.getSuccess();
        return (returnVal);
    }





}