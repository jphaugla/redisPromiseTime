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
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.stereotype.Service;

import java.net.InetAddress;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@Slf4j
@Service
public class OrderStreamListener implements StreamListener<String, ObjectRecord<String, OrderEvent>> {

    @Value("${stream.key:order-events}")
    private String streamKey;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private RedisTemplate<String, String> redisTemplate;

    @Override
    @SneakyThrows
    public void onMessage(ObjectRecord<String, OrderEvent> record) {
        OrderEvent orderEvent = record.getValue();
        log.info(InetAddress.getLocalHost().getHostName() + " - consumed :" + orderEvent);

        if ( StringUtils.isEmpty(orderEvent.getVerificationTime())) {
            //  since no verification time, this is the initial record
            redisTemplate.opsForZSet().add(getUnfilledZsetFacilityKey(orderEvent),
                    orderEvent.getWorkOrderId(), convertTime(orderEvent.getPromiseTime()));
        } else {
            //  this is the final record with the verification time
            //  set Success Flag in orderEvent
            orderEvent.setSuccess(getSuccess(orderEvent.getPromiseTime(), orderEvent.getVerificationTime()));
            // completed/Filled work orders per store zset with hash key as member value and promise time as the score
            redisTemplate.opsForZSet().add(getFilledZsetFacilityKey(orderEvent),
                    getKeyValue(orderEvent), convertTime(orderEvent.getPromiseTime()));
            //  if success, add counter for the store in success zset with store as member value and success count as score
            if(orderEvent.getSuccess().equals("0")) {
                //  score
                // redisTemplate.opsForZSet().incrementScore(getStoreSuccess(orderEvent),
                //        orderEvent.getFacilityId(),1);
                redisTemplate.opsForHash().increment(getStoreSuccessFailure(orderEvent),"success", 1);
            } else {
                //  if failure, add counter for the store in failure zset with store as member value and success count as score
                // redisTemplate.opsForZSet().incrementScore(getStoreFailure(orderEvent),
                 //       orderEvent.getFacilityId(),1);
                redisTemplate.opsForHash().increment(getStoreSuccessFailure(orderEvent),"failure", 1);

            }
            //  this zset holds all the records with semicolon separators as the value and verification time as score
            redisTemplate.opsForZSet().add(getFullRecordZsetKey(orderEvent), getAllRecords(orderEvent),
                    convertTime(orderEvent.getVerificationTime()));
        }
        //  write to the hash either way.  This hash is main record of all the transactions
        ObjectMapper mapObject = new ObjectMapper();
        Map < Object, Object > orderHash = mapObject.convertValue(orderEvent, Map.class);
        redisTemplate.opsForHash().putAll(getKeyValue(orderEvent), orderHash);
        redisTemplate.opsForStream().acknowledge(streamKey, record);
    }
    private long convertTime (String inTime) throws ParseException {
        SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = dateFormatter.parse(inTime);
        long msec = date.getTime();
        System.out.println("Epoch of the given date: "+msec);
        return(msec);
    }
    private String getSuccess(String promiseTime, String verificationTime) throws ParseException {
        String returnVal;
        if (convertTime(promiseTime) <= convertTime(verificationTime)) {
            returnVal = "0";
        } else {
            returnVal = "1";
        }
        return (returnVal);
    }
    public String getKeyValue(OrderEvent inOrder) {
        return ("Order:" + inOrder.getFacilityId() + ":" + inOrder.getWorkOrderId()
                + ":" + inOrder.getPromiseTime());
    }
    public String getUnfilledZsetFacilityKey(OrderEvent inOrder) {
        return("Unfilled:" + inOrder.getFacilityId() + ":" + inOrder.getQueue());
    }
    public String getFilledZsetFacilityKey(OrderEvent inOrder) {
        return("Filled:" + inOrder.getFacilityId() + ":" + inOrder.getQueue());
    }

    public String getStoreSuccess(OrderEvent inOrder) {
        return ("Success:" + inOrder.getQueue());
    }

    public String getStoreFailure(OrderEvent inOrder) {
        return ("Failure:" + inOrder.getQueue());
    }

    public String getFullRecordZsetKey(OrderEvent inOrder) {
        return ("FullStoreRecord:" + inOrder.getFacilityId());
    }

    public String getAllRecords(OrderEvent inOrder) {
        String returnVal = inOrder.getFacilityId() + ":" + inOrder.getWorkOrderId() + ":"
                + inOrder.getQueue() + ":" + inOrder.getPromiseTime() + ":" + inOrder.getVerificationTime();
        return (returnVal);
    }

    public String getStoreSuccessFailure(OrderEvent inOrder)  {
        return ("StoreSuccess:" + inOrder.getFacilityId());
    }


}