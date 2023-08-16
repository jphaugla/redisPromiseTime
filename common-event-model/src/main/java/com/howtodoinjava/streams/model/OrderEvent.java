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


}
