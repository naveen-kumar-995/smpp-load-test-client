package com.ptsl.beacon;

import com.cloudhopper.commons.charset.CharsetUtil;
import com.cloudhopper.smpp.*;
import com.cloudhopper.smpp.impl.DefaultSmppClient;
import com.cloudhopper.smpp.impl.DefaultSmppSessionHandler;
import com.cloudhopper.smpp.pdu.*;
import com.cloudhopper.smpp.tlv.Tlv;
import com.cloudhopper.smpp.type.Address;
import com.cloudhopper.smpp.type.SmppInvalidArgumentException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class MultiPartLoadPush {

    private static final Logger log = LoggerFactory.getLogger(MultiPartLoadPush.class);

    private static final int SESSIONS = getIntEnv("SESSIONS", 5);
    private static final int WINDOW = getIntEnv("WINDOW_SIZE", 1000);
    private static final int WORKERS = getIntEnv("WORKER_THREADS", 50);
    private static final int QUEUE_SIZE = getIntEnv("QUEUE_SIZE", 100000);

    private static final String HOST = getEnv("SMSC_HOST", "127.0.0.1");
    private static final int PORT = getIntEnv("SMSC_PORT", 2775);
    private static final String SYSTEM_ID = getEnv("SMSC_SYSTEM_ID", "test");
    private static final String PASSWORD = getEnv("SMSC_PASSWORD", "test");

    private static final long MAX_MESSAGES = getLongEnv("MAX_MESSAGES", 1000000);

    private static final BlockingQueue<SmsFragment> queue =
            new ArrayBlockingQueue<>(QUEUE_SIZE);

    private static final AtomicInteger REF_GEN = new AtomicInteger();

    private static final AtomicLong produced = new AtomicLong();
    private static final AtomicLong sent = new AtomicLong();
    private static final AtomicLong success = new AtomicLong();
    private static final AtomicLong failed = new AtomicLong();

    private static SmppSession[] sessions;

    public static void main(String[] args) throws Exception {

        log.error("Starting SMPP Multipart Load Generator");

        ExecutorService smppExecutor = Executors.newCachedThreadPool();
        DefaultSmppClient client = new DefaultSmppClient(smppExecutor, SESSIONS);

        sessions = new SmppSession[SESSIONS];

        for (int i = 0; i < SESSIONS; i++) {

            SmppSessionConfiguration cfg = new SmppSessionConfiguration();
            cfg.setName("session-" + i);
            cfg.setType(SmppBindType.TRANSCEIVER);
            cfg.setHost(HOST);
            cfg.setPort(PORT);
            cfg.setSystemId(SYSTEM_ID);
            cfg.setPassword(PASSWORD);
            cfg.setWindowSize(WINDOW);

            sessions[i] = client.bind(cfg, new Handler());
        }

        log.error("Connected {} sessions", SESSIONS);

        startGenerator();
        startSenders();
        startMetrics();
    }

    // --------------------------------------------------------
    // MESSAGE GENERATOR
    // --------------------------------------------------------

    static void startGenerator() {

        Thread generator = new Thread(() -> {

            String message =
                    "An amount of {#var#} has been debited to {#var#} on {#var#} towards {#var#} "
                            + "fvg Benf {#var#}, IFSC {#var#}, Benf A/c {#var#}, UTR {#var#}. "
                            + "Total Avail. Bal INR {#var#} -Canara Bank";

            while (produced.get() < MAX_MESSAGES) {

                Address src = randomSource();
                Address dst = randomDestination();

                int ref = REF_GEN.incrementAndGet() & 0xffff;

                List<SmsFragment> fragments =
                        buildMultipart(message, src, dst, ref);

                for (SmsFragment fragment : fragments) {

                    long next = produced.incrementAndGet();

                    if (next > MAX_MESSAGES)
                        return;

                    try {
                        queue.put(fragment);
                    } catch (Exception ignored) {}
                }
            }

            log.error("Generator finished producing messages");
        });

        generator.setDaemon(true);
        generator.start();
    }

    // --------------------------------------------------------
    // SENDERS
    // --------------------------------------------------------

    static void startSenders() {

        ExecutorService workers =
                Executors.newThreadPerTaskExecutor(Thread.ofVirtual().factory());

        for (int i = 0; i < WORKERS; i++) {

            workers.submit(() -> {

                int sessionIndex = 0;

                while (true) {

                    try {

                        SmsFragment fragment = queue.take();

                        SmppSession session = sessions[sessionIndex];

                        sessionIndex++;
                        if (sessionIndex == SESSIONS)
                            sessionIndex = 0;

                        if (session == null || !session.isBound())
                            continue;

                        SubmitSm sm = createSubmitSm(fragment);

                        SubmitSmResp resp = session.submit(sm, 10000);

                        sent.incrementAndGet();

                        if (resp.getCommandStatus() == 0)
                            success.incrementAndGet();
                        else
                            failed.incrementAndGet();

                    } catch (Exception e) {
                        failed.incrementAndGet();
                    }
                }
            });
        }
    }

    // --------------------------------------------------------
    // SUBMITSM BUILDER
    // --------------------------------------------------------

    private static SubmitSm createSubmitSm(SmsFragment fragment) {

        SubmitSm sm = new SubmitSm();

        sm.setSourceAddress(fragment.source);
        sm.setDestAddress(fragment.destination);

        sm.setDataCoding((byte) 0);

        sm.setRegisteredDelivery(
                SmppConstants.REGISTERED_DELIVERY_SMSC_RECEIPT_REQUESTED);

        if (fragment.hasUdh)
            sm.setEsmClass(SmppConstants.ESM_CLASS_UDHI_MASK);

        try {
            sm.setShortMessage(fragment.payload);
        } catch (SmppInvalidArgumentException e) {
            throw new RuntimeException(e);
        }

        sm.addOptionalParameter(new Tlv((short) 0x1400, "110100001403".getBytes()));
        sm.addOptionalParameter(new Tlv((short) 0x1401, "1107174074670190034".getBytes()));

        return sm;
    }

    // --------------------------------------------------------
    // MULTIPART BUILDER
    // --------------------------------------------------------

    static List<SmsFragment> buildMultipart(
            String message,
            Address source,
            Address destination,
            int ref) {

        List<SmsFragment> parts = new ArrayList<>();

        byte[] msgBytes =
                CharsetUtil.encode(message, CharsetUtil.CHARSET_GSM);

        int multiLimit = 153;

        int totalParts =
                (int) Math.ceil((double) msgBytes.length / multiLimit);

        for (int part = 1; part <= totalParts; part++) {

            int start = (part - 1) * multiLimit;
            int len = Math.min(multiLimit, msgBytes.length - start);

            byte[] body = new byte[len];
            System.arraycopy(msgBytes, start, body, 0, len);

            byte[] udh = createUdh(ref, totalParts, part);

            ByteBuffer buffer =
                    ByteBuffer.allocate(udh.length + body.length);

            buffer.put(udh);
            buffer.put(body);

            parts.add(new SmsFragment(
                    buffer.array(),
                    true,
                    source,
                    destination));
        }

        return parts;
    }

    private static byte[] createUdh(int ref, int total, int seq) {

        return new byte[]{
                0x05,
                0x00,
                0x03,
                (byte) ref,
                (byte) total,
                (byte) seq
        };
    }

    // --------------------------------------------------------
    // METRICS
    // --------------------------------------------------------

    static void startMetrics() {

        ScheduledExecutorService metrics =
                Executors.newScheduledThreadPool(1);

        AtomicLong lastSent = new AtomicLong();

        metrics.scheduleAtFixedRate(() -> {

            long current = sent.get();
            long delta = current - lastSent.getAndSet(current);

            log.error(
                    "TPS={} sent={} success={} failed={} queue={}",
                    delta / 5,
                    delta,
                    success.get(),
                    failed.get(),
                    queue.size());

        }, 5, 5, TimeUnit.SECONDS);
    }

    // --------------------------------------------------------
    // DATA MODEL
    // --------------------------------------------------------

    static class SmsFragment {

        byte[] payload;
        boolean hasUdh;
        Address source;
        Address destination;

        SmsFragment(
                byte[] payload,
                boolean hasUdh,
                Address source,
                Address destination) {

            this.payload = payload;
            this.hasUdh = hasUdh;
            this.source = source;
            this.destination = destination;
        }
    }

    // --------------------------------------------------------
    // SESSION HANDLER
    // --------------------------------------------------------

    static class Handler extends DefaultSmppSessionHandler {

        @Override
        public void fireChannelUnexpectedlyClosed() {
            log.error("SMPP session closed");
        }
    }

    // --------------------------------------------------------
    // RANDOM ADDRESS GENERATORS
    // --------------------------------------------------------

    private static Address randomDestination() {

        long number =
                910000000000L +
                        ThreadLocalRandom.current().nextLong(999999999L);

        return new Address((byte) 1, (byte) 1, String.valueOf(number));
    }

    private static final String[] HEADERS = {
            "CANBNK","HDFCBK","ICICIB","AXISBK","SBIOTP",
            "PAYTMB","AMAZON","FLIPKT","MYBANK","UPIOTP"
    };

    private static Address randomSource() {

        String header =
                HEADERS[ThreadLocalRandom.current().nextInt(HEADERS.length)];

        return new Address((byte) 1, (byte) 1, header);
    }

    // --------------------------------------------------------
    // ENV HELPERS
    // --------------------------------------------------------

    private static String getEnv(String key,String def){
        String v = System.getenv(key);
        return (v==null||v.isBlank()) ? def : v.trim();
    }

    private static int getIntEnv(String key,int def){
        try{
            return Integer.parseInt(getEnv(key,String.valueOf(def)));
        }catch(Exception e){
            return def;
        }
    }

    private static long getLongEnv(String key,long def){
        try{
            return Long.parseLong(getEnv(key,String.valueOf(def)));
        }catch(Exception e){
            return def;
        }
    }
}
