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

public class SmppMaxLoadClient {

    private static final Logger log = LoggerFactory.getLogger(SmppMaxLoadClient.class);
    private static final int SESSIONS =
            Integer.parseInt(getConfig("SESSIONS", "20"));

    private static final int WINDOW_SIZE =
            Integer.parseInt(getConfig("WINDOW_SIZE", "1000"));

    private static final int WORKERS =
            Integer.parseInt(getConfig("WORKER_THREADS", "200"));

    private static final String HOST =
            getConfig("SMSC_HOST", "127.0.0.1");

    private static final int PORT =
            Integer.parseInt(getConfig("SMSC_PORT", "2775"));

    private static final String SYSTEM_ID =
            getConfig("SMSC_SYSTEM_ID", "test");

    private static final String PASSWORD =
            getConfig("SMSC_PASSWORD", "test");

    private static final AtomicLong sent = new AtomicLong();
    private static final AtomicLong success = new AtomicLong();
    private static final AtomicLong failed = new AtomicLong();

    private static SmppSession[] sessions;

    public static void main(String[] args) throws Exception {

        logStartupConfig();

        ExecutorService smppExecutor =
                Executors.newFixedThreadPool(Math.max(4, SESSIONS));

        ScheduledExecutorService scheduler =
                Executors.newScheduledThreadPool(2);

        DefaultSmppClient client =
                new DefaultSmppClient(smppExecutor, SESSIONS);

        sessions = new SmppSession[SESSIONS];

        for (int i = 0; i < SESSIONS; i++) {

            SmppSessionConfiguration config = new SmppSessionConfiguration();

            config.setName("load-session-" + i);
            config.setType(SmppBindType.TRANSCEIVER);
            config.setHost(HOST);
            config.setPort(PORT);
            config.setSystemId(SYSTEM_ID);
            config.setPassword(PASSWORD);

            config.setWindowSize(WINDOW_SIZE);
            config.setConnectTimeout(10000);

            config.getLoggingOptions().setLogBytes(false);
            config.getLoggingOptions().setLogPdu(false);

            sessions[i] = client.bind(config, new LoadSessionHandler());
        }

        log.debug("Connected SMPP sessions: " + SESSIONS);

        byte[] message =
                CharsetUtil.encode("Load test message", CharsetUtil.CHARSET_GSM);

        ExecutorService virtualWorkers =
                Executors.newVirtualThreadPerTaskExecutor();

        for (int i = 0; i < WORKERS; i++) {

            virtualWorkers.submit(() -> {

                int index = 0;

                while (true) {

                    SmppSession session = sessions[index++ % SESSIONS];

                    if (session == null || !session.isBound()) {
                        Thread.onSpinWait();
                        continue;
                    }

                    SubmitSm sm = new SubmitSm();

                    sm.setSourceAddress(new Address((byte)0x01,(byte)0x01,"TEST"));
                    sm.setDestAddress(new Address((byte)0x01,(byte)0x01,"917550232158"));
                    sm.setShortMessage(message);
                    sm.setDataCoding((byte)0x00);

                    try {

                        session.sendRequestPdu(sm, 10000, false);

                        sent.incrementAndGet();

                    } catch (Exception e) {

                        failed.incrementAndGet();

                    }
                }
            });
        }

        // Throughput metrics
        scheduler.scheduleAtFixedRate(() -> {

            long s = sent.getAndSet(0);
            long ok = success.getAndSet(0);
            long err = failed.getAndSet(0);

            log.debug(
                    "[METRICS] TPS=%d success=%d failed=%d%n",
                    s, ok, err);

        }, 5, 5, TimeUnit.SECONDS);


        // Watchdog thread
        scheduler.scheduleAtFixedRate(() -> {

            int bound = 0;

            for (SmppSession session : sessions) {
                if (session != null && session.isBound()) {
                    bound++;
                }
            }

            log.debug(
                    "[WATCHDOG] boundSessions=%d/%d | totalSent=%d | totalSuccess=%d | totalFailed=%d%n",
                    bound,
                    SESSIONS,
                    sent.get(),
                    success.get(),
                    failed.get());

        }, 10, 10, TimeUnit.SECONDS);
    }


    static class LoadSessionHandler extends DefaultSmppSessionHandler {

        @Override
        public void fireExpectedPduResponseReceived(PduAsyncResponse asyncResponse) {

            if (asyncResponse.getResponse().getCommandStatus() == 0) {
                success.incrementAndGet();
            } else {
                failed.incrementAndGet();
            }
        }

        @Override
        public void fireChannelUnexpectedlyClosed() {
            log.debug("[WARN] SMPP session closed unexpectedly");
        }
    }


    private static String getConfig(String key, String defaultValue) {

        String env = System.getenv(key);
        if (env != null && !env.isEmpty()) {
            return env.trim();
        }

        String prop = System.getProperty(key);
        if (prop != null && !prop.isEmpty()) {
            return prop.trim();
        }

        return defaultValue;
    }


    private static void logStartupConfig() {

        log.debug("===== SMPP LOAD CONFIG =====");

        log.debug("Host           : {}", HOST);
        log.debug("Port           : {}", PORT);
        log.debug("SystemId       : {}", SYSTEM_ID);

        log.debug("Sessions       : {}", SESSIONS);
        log.debug("WindowSize     : {}", WINDOW_SIZE);
        log.debug("VirtualWorkers : {}", WORKERS);

        log.debug("============================");
    }
}