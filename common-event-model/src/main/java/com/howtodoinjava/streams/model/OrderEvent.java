package com.howtodoinjava.streams.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class OrderEvent {

    private String facilityId;
    private String workOrderId;
    private String promiseTime;
    private String verificationTime;
    private String queue;
    private String success;
    public static long convertTime (String inTime) throws ParseException {
        SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = dateFormatter.parse(inTime);
        long msec = date.getTime();
       // System.out.println("Epoch of the given date: "+msec);
        return(msec);
    }
    public static String getKeyNameStoreSuccess(String inFacilityId, String inQueue)  {
        return ("StoreSuccess:" + inFacilityId + ":" + inQueue);
    }
    public static String getKeyNameStoreSuccess(String inFacilityIdQueue)  {
        return ("StoreSuccess:" + inFacilityIdQueue);
    }
    public static String getKeyNameOrderPromise(String inFacilityId, String inWorkOrderId, String inPromiseTime) {
        return ("Order:" + inFacilityId + ":" + inWorkOrderId
                + ":" + inPromiseTime);
    }
    public static String getKeyNameUnfilled(String inFacilityId, String inQueue) {
        return("Unfilled:" + inFacilityId + ":" + inQueue);
    }
    public static String getKeyNameFullPromise(String inFacilityId, String inQueue) {
        return("FullStorePromise:" + inFacilityId + ":" + inQueue);
    }

    public static String getKeyNameFullVerif(String inFacilityId, String inQueue) {
        return ("FullStoreVerif:" + inFacilityId + ":" + inQueue);
    }

    public static String getKeyNameAllStores() {
        return ("AllStores");
    }

}
