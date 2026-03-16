package com.ptsl.beacon;

import com.cloudhopper.smpp.*;
import com.cloudhopper.smpp.impl.DefaultSmppClient;
import com.cloudhopper.smpp.impl.DefaultSmppSessionHandler;
import com.cloudhopper.smpp.pdu.*;
import com.cloudhopper.smpp.type.Address;
import com.cloudhopper.commons.charset.CharsetUtil;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TelecomGradeSmppLoad {

    private static final Logger log = LoggerFactory.getLogger(TelecomGradeSmppLoad.class);

    private static final int SESSIONS =
            getIntEnv("SESSIONS", 10);

    private static final int WINDOW =
            getIntEnv("WINDOW_SIZE", 1000);

    private static final int WORKERS =
            getIntEnv("WORKER_THREADS", 200);

    private static final int QUEUE_SIZE =
            getIntEnv("QUEUE_SIZE", 100000);

    private static final String HOST =
            getEnv("SMSC_HOST", "127.0.0.1");

    private static final int PORT =
            getIntEnv("SMSC_PORT", 2775);

    private static final String SYSTEM_ID =
            getEnv("SMSC_SYSTEM_ID", "test");

    private static final String PASSWORD =
            getEnv("SMSC_PASSWORD", "test");

    private static final long MAX_MESSAGES =
            getLongEnv("MAX_MESSAGES", 1000000);

    private static BlockingQueue<byte[]> queue =
            new ArrayBlockingQueue<>(QUEUE_SIZE);

    private static AtomicLong sent = new AtomicLong();
    private static AtomicLong success = new AtomicLong();
    private static AtomicLong failed = new AtomicLong();

    private static SmppSession[] sessions;

    private static void printConfig() {

        log.error("===== SMPP LOAD CONFIG =====");

        log.error("Host: {}", HOST);
        log.error("Port: {}", PORT);
        log.error("SystemId: {}", SYSTEM_ID);

        log.error("Sessions: {}", SESSIONS);
        log.error("Window: {}", WINDOW);
        log.error("Workers: {}", WORKERS);
        log.error("Queue: {}", QUEUE_SIZE);

        log.error("============================");
    }

    public static void main(String[] args) throws Exception {

        printConfig();

        ExecutorService smppExecutor = Executors.newCachedThreadPool();

        DefaultSmppClient client =
                new DefaultSmppClient(smppExecutor, SESSIONS);

        sessions = new SmppSession[SESSIONS];

        for(int i=0;i<SESSIONS;i++){

            SmppSessionConfiguration cfg = new SmppSessionConfiguration();

            cfg.setName("session-"+i);
            cfg.setType(SmppBindType.TRANSCEIVER);
            cfg.setHost(HOST);
            cfg.setPort(PORT);
            cfg.setSystemId(SYSTEM_ID);
            cfg.setPassword(PASSWORD);

            cfg.setWindowSize(WINDOW);
            cfg.setConnectTimeout(10000);
            cfg.setRequestExpiryTimeout(30000);
            cfg.setWindowMonitorInterval(15000);

            sessions[i] = client.bind(cfg,new Handler());
        }

        log.error("Sessions connected: {}", SESSIONS);

        startGenerator();
        startSenders();
        startMetrics();
//        startWatchdog();
    }

    static void startGenerator(){

        Thread generator = new Thread(() -> {

            byte[] msg = CharsetUtil.encode(
                    "LOADTESTSINGLEPARTMESSAGE",
                    CharsetUtil.CHARSET_GSM);

            while(sent.get() < MAX_MESSAGES){

                try{
                    queue.put(msg);
                }
                catch(Exception ignored){}
            }

            log.error("Generator finished producing {} messages", MAX_MESSAGES);

        });

        generator.setName("message-generator");
        generator.setDaemon(true);
        generator.start();
    }


    static void startSenders(){

        ExecutorService workers =
                Executors.newThreadPerTaskExecutor(Thread.ofVirtual().factory());

        for(int i=0;i<WORKERS;i++){

            workers.submit(() -> {

                int index = 0;

                while(true){

                    try{

                        byte[] payload = queue.take();

                        SmppSession session =
                                sessions[index++ % SESSIONS];

                        if(session==null || !session.isBound())
                            continue;

                        SubmitSm sm = new SubmitSm();

                        sm.setSourceAddress(
                                new Address((byte)1,(byte)1,"TEST"));

                        sm.setDestAddress(
                                new Address((byte)1,(byte)1,"917550232158"));

                        sm.setShortMessage(payload);
                        sm.setDataCoding((byte)0);

                        SubmitSmResp resp = session.submit(sm,10000);

                        sent.incrementAndGet();

                        if(resp.getCommandStatus()==0)
                            success.incrementAndGet();
                        else
                            failed.incrementAndGet();

                    }
                    catch(Exception e){

                        failed.incrementAndGet();

                    }

                }

            });
        }
    }

    static void startMetrics(){

        ScheduledExecutorService metrics =
                Executors.newScheduledThreadPool(1);

        final AtomicLong lastSent = new AtomicLong();
        final AtomicLong lastSuccess = new AtomicLong();
        final AtomicLong lastFailed = new AtomicLong();

        metrics.scheduleAtFixedRate(() -> {

            long currentSent = sent.get();
            long currentSuccess = success.get();
            long currentFailed = failed.get();

            long sentDelta = currentSent - lastSent.getAndSet(currentSent);
            long successDelta = currentSuccess - lastSuccess.getAndSet(currentSuccess);
            long failedDelta = currentFailed - lastFailed.getAndSet(currentFailed);

            long tps = sentDelta / 5;

            log.error(
                    "TPS={} sent={} success={} failed={} queue={} totalSent={}",
                    tps,
                    sentDelta,
                    successDelta,
                    failedDelta,
                    queue.size(),
                    currentSent
                     );

            int bound=0;

            for(SmppSession s : sessions) {
                if (s != null && s.isBound()) {
                    bound++;
                }
                log.error("Live sessions={}/{}", bound, SESSIONS);
            }
        },5,5,TimeUnit.SECONDS);
    }


//    static void startWatchdog(){
//
//        ScheduledExecutorService wd =
//                Executors.newScheduledThreadPool(1);
//
//        wd.scheduleAtFixedRate(() -> {
//
//            int bound=0;
//
//            for(SmppSession s : sessions)
//                if(s!=null && s.isBound())
//                    bound++;
//
//            log.error("WATCHDOG sessions={}/{}", bound, SESSIONS);
//
//        },10,10,TimeUnit.SECONDS);
//    }

    static class Handler extends DefaultSmppSessionHandler implements com.ptsl.beacon.Handler {

        @Override
        public void fireExpectedPduResponseReceived(
                PduRequest req,
                PduResponse resp){

            if(resp.getCommandStatus()==0)
                success.incrementAndGet();
            else
                failed.incrementAndGet();
        }

        @Override
        public void fireChannelUnexpectedlyClosed() {

            log.error("Session closed unexpectedly");
        }
    }

    private static String getEnv(String key, String defaultValue) {

        String value = System.getenv(key);

        if (value == null || value.isBlank()) {
            return defaultValue;
        }

        return value.trim();
    }

    private static int getIntEnv(String key, int defaultValue) {

        try {
            return Integer.parseInt(getEnv(key, String.valueOf(defaultValue)));
        } catch (Exception e) {
            return defaultValue;
        }
    }

    private static long getLongEnv(String key, long defaultValue) {

        try {
            return Long.parseLong(getEnv(key, String.valueOf(defaultValue)));
        } catch (Exception e) {
            return defaultValue;
        }
    }
}
