package com.ptsl.beacon;

import com.cloudhopper.smpp.*;
import com.cloudhopper.smpp.impl.DefaultSmppClient;
import com.cloudhopper.smpp.impl.DefaultSmppSessionHandler;
import com.cloudhopper.smpp.pdu.*;
import com.cloudhopper.smpp.type.Address;
import com.cloudhopper.commons.charset.CharsetUtil;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class TelecomGradeSmppLoad {

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

    private static BlockingQueue<byte[]> queue =
            new ArrayBlockingQueue<>(QUEUE_SIZE);

    private static AtomicLong sent = new AtomicLong();
    private static AtomicLong success = new AtomicLong();
    private static AtomicLong failed = new AtomicLong();

    private static SmppSession[] sessions;

    private static void printConfig() {

        System.out.println("===== SMPP LOAD CONFIG =====");

        System.out.println("Host: " + HOST);
        System.out.println("Port: " + PORT);
        System.out.println("SystemId: " + SYSTEM_ID);

        System.out.println("Sessions: " + SESSIONS);
        System.out.println("Window: " + WINDOW);
        System.out.println("Workers: " + WORKERS);
        System.out.println("Queue: " + QUEUE_SIZE);

        System.out.println("============================");
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

            sessions[i] = client.bind(cfg,new Handler());
        }

        System.out.println("Sessions connected: "+SESSIONS);

        startGenerator();
        startSenders();
        startMetrics();
        startWatchdog();
    }

    static void startGenerator(){

        new Thread(() -> {

            byte[] msg = CharsetUtil.encode(
                    "LOADTEST",
                    CharsetUtil.CHARSET_GSM);

            while(true){

                try{
                    queue.put(msg);
                }
                catch(Exception ignored){}

            }

        }).start();
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

                        if(session.getSendWindow().getFreeSize()==0){

                            queue.offer(payload);
                            Thread.sleep(1);
                            continue;
                        }

                        SubmitSm sm = new SubmitSm();

                        sm.setSourceAddress(
                                new Address((byte)1,(byte)1,"TEST"));

                        sm.setDestAddress(
                                new Address((byte)1,(byte)1,"917550232158"));

                        sm.setShortMessage(payload);

                        session.sendRequestPdu(sm,10000,false);

                        sent.incrementAndGet();

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

        metrics.scheduleAtFixedRate(() -> {

            long s = sent.getAndSet(0);
            long ok = success.getAndSet(0);
            long err = failed.getAndSet(0);

            System.out.println(
                    "TPS="+s+
                            " success="+ok+
                            " failed="+err+
                            " queue="+queue.size());

        },5,5,TimeUnit.SECONDS);
    }

    static void startWatchdog(){

        ScheduledExecutorService wd =
                Executors.newScheduledThreadPool(1);

        wd.scheduleAtFixedRate(() -> {

            int bound=0;

            for(SmppSession s : sessions)
                if(s!=null && s.isBound())
                    bound++;

            System.out.println(
                    "WATCHDOG sessions="+bound+"/"+SESSIONS);

        },10,10,TimeUnit.SECONDS);
    }

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

}
