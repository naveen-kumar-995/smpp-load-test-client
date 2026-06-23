package com.ptsl.beacon;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StartApplication
{

    private static final Logger log = LoggerFactory.getLogger(StartApplication.class);

    public static void main(String[] args) {

        try
        {
            log.debug("Starting Async SMPP load test..");
            com.ptsl.beacon.async.AsyncMultiPartLoadPush.main(args);
        } catch (Exception e) {
            log.error("Exception while starting the load test", e);
        }
    }
}
