package com.ptsl.beacon;

import com.cloudhopper.commons.charset.CharsetUtil;
import com.cloudhopper.smpp.*;
import com.cloudhopper.smpp.impl.DefaultSmppClient;
import com.cloudhopper.smpp.impl.DefaultSmppSessionHandler;
import com.cloudhopper.smpp.pdu.DeliverSm;
import com.cloudhopper.smpp.pdu.PduRequest;
import com.cloudhopper.smpp.pdu.PduResponse;
import com.cloudhopper.smpp.pdu.SubmitSm;
import com.cloudhopper.smpp.pdu.SubmitSmResp;
import com.cloudhopper.smpp.tlv.Tlv;
import com.cloudhopper.smpp.type.Address;
import com.cloudhopper.smpp.type.SmppInvalidArgumentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.*;
import java.util.concurrent.ThreadLocalRandom;
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

    /**
     * MAX_MESSAGES means total PDUs to submit, not logical SMS count.
     * Example:
     * - single-part SMS = 1 PDU
     * - 2-part multipart SMS = 2 PDUs
     */
    private static final long MAX_MESSAGES = getLongEnv("MAX_MESSAGES", 1_000_000);

    /**
     * MESSAGE_MODE:
     *  - single
     *  - multipart
     *  - mixed
     */
    private static final String MESSAGE_MODE =
            getEnv("MESSAGE_MODE", "multipart").trim().toLowerCase(Locale.ROOT);

    /**
     * Used only when MESSAGE_MODE=mixed
     * Example SINGLE_PERCENT=30 => 30% single, 70% multipart
     */
    private static final int SINGLE_PERCENT =
            Math.max(0, Math.min(100, getIntEnv("SINGLE_PERCENT", 50)));

    private static final boolean UNICODE =
            getBooleanEnv("UNICODE", false);

    private static final AtomicInteger REF_GEN = new AtomicInteger();

    private static final BlockingQueue<SmsFragment> queue =
            new ArrayBlockingQueue<>(QUEUE_SIZE);

    private static final AtomicLong produced = new AtomicLong();
    private static final AtomicLong sent = new AtomicLong();
    private static final AtomicLong success = new AtomicLong();
    private static final AtomicLong failed = new AtomicLong();

    private static final AtomicLong generatedSingleMessages = new AtomicLong();
    private static final AtomicLong generatedMultipartMessages = new AtomicLong();

    private static AtomicLong dlrReceived = new AtomicLong();

    private static SmppSession[] sessions;

    private static final String SINGLE_MESSAGE =
            getEnv("SINGLE_MESSAGE",
                    "Canara Bank alert: INR 1000 credited to your account. Ref {#var#}");

    private static final String MULTIPART_MESSAGE =
            getEnv("MULTIPART_MESSAGE",
                    "An amount of {#var#} has been debited to {#var#} on {#var#} towards {#var#} "
                            + "fvg Benf {#var#}, IFSC {#var#}, Benf A/c {#var#}, UTR {#var#}. "
                            + "Total Avail. Bal INR {#var#} -Canara Bank");

    private static final String[] HEADERS = {
            "CANBNK", "HDFCBK", "ICICIB", "AXISBK", "SBIOTP",
            "PAYTMB", "AMAZON", "FLIPKT", "MYBANK", "UPIOTP"
    };

    public static void main(String[] args) throws Exception {

        printConfig();

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

        log.error("Connected {} SMPP sessions", SESSIONS);

        startGenerator();
        startSenders();
        startMetrics();
    }

    private static void printConfig() {
        log.error("========== SMPP LOAD CONFIG ==========");
        log.error("Host={}", HOST);
        log.error("Port={}", PORT);
        log.error("Sessions={}", SESSIONS);
        log.error("Window={}", WINDOW);
        log.error("Workers={}", WORKERS);
        log.error("QueueSize={}", QUEUE_SIZE);
        log.error("MaxMessages(PDUs)={}", MAX_MESSAGES);
        log.error("MessageMode={}", MESSAGE_MODE);
        log.error("SinglePercent={}", SINGLE_PERCENT);
        log.error("Unicode={}", UNICODE);
        log.error("======================================");
    }

    // --------------------------------------------------------
    // MESSAGE GENERATOR
    // --------------------------------------------------------

    static void startGenerator() {

        Thread generator = new Thread(() -> {
            while (produced.get() < MAX_MESSAGES) {

                Address src = randomSource();
                Address dst = randomDestination();

                List<SmsFragment> fragments = createNextMessage(src, dst);

                for (SmsFragment fragment : fragments) {
                    long next = produced.incrementAndGet();

                    if (next > MAX_MESSAGES) {
                        return;
                    }

                    try {
                        queue.put(fragment);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }

            log.error("Generator finished producing PDUs. produced={}", produced.get());
        });

        generator.setName("message-generator");
        generator.setDaemon(true);
        generator.start();
    }

    private static List<SmsFragment> createNextMessage(Address source, Address destination) {
        boolean sendSingle = shouldSendSingle();

        if (sendSingle) {
            generatedSingleMessages.incrementAndGet();
            return buildSinglePart(SINGLE_MESSAGE, source, destination, UNICODE);
        } else {
            generatedMultipartMessages.incrementAndGet();
            int ref = REF_GEN.incrementAndGet() & 0xFF; // 8-bit concat ref for IEI 0x00
            return buildMultipart(MULTIPART_MESSAGE, source, destination, ref, UNICODE);
        }
    }

    private static boolean shouldSendSingle() {
        return switch (MESSAGE_MODE) {
            case "single" -> true;
            case "multipart" -> false;
            case "mixed" -> ThreadLocalRandom.current().nextInt(100) < SINGLE_PERCENT;
            default -> false;
        };
    }

    // --------------------------------------------------------
    // SENDERS
    // --------------------------------------------------------

    static void startSenders() {
        ExecutorService workers =
                Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("smpp-worker-", 0).factory());

        for (int i = 0; i < WORKERS; i++) {
            workers.submit(() -> {
                int sessionIndex = 0;

                while (true) {
                    try {
                        SmsFragment fragment = queue.take();

                        SmppSession session = sessions[sessionIndex];
                        sessionIndex++;
                        if (sessionIndex == SESSIONS) {
                            sessionIndex = 0;
                        }

                        if (session == null || !session.isBound()) {
                            failed.incrementAndGet();
                            continue;
                        }

                        SubmitSm sm = createSubmitSm(fragment);
                        SubmitSmResp resp = session.submit(sm, 10_000);

                        sent.incrementAndGet();

                        if (resp.getCommandStatus() == SmppConstants.STATUS_OK) {
                            success.incrementAndGet();
                        } else {
                            failed.incrementAndGet();
                        }

                    } catch (Exception e) {
                        failed.incrementAndGet();
                        log.debug("Submit failed", e);
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
        sm.setDataCoding(fragment.dataCoding);
        sm.setRegisteredDelivery(SmppConstants.REGISTERED_DELIVERY_SMSC_RECEIPT_REQUESTED);

        if (fragment.hasUdh) {
            sm.setEsmClass(SmppConstants.ESM_CLASS_UDHI_MASK);
        } else {
            sm.setEsmClass((byte) 0x00);
        }

        try {
            sm.setShortMessage(fragment.payload);
        } catch (SmppInvalidArgumentException e) {
            throw new RuntimeException("Failed to set short_message", e);
        }

        sm.addOptionalParameter(new Tlv((short) 0x1400, "110100001403".getBytes()));
        sm.addOptionalParameter(new Tlv((short) 0x1401, "1107174074670670034".getBytes()));

        return sm;
    }

    // --------------------------------------------------------
    // SINGLE PART BUILDER
    // --------------------------------------------------------

    static List<SmsFragment> buildSinglePart(
            String message,
            Address source,
            Address destination,
            boolean unicode) {

        List<SmsFragment> parts = new ArrayList<>(1);

        byte dataCoding = unicode ? (byte) 0x08 : (byte) 0x00;
        byte[] msgBytes = CharsetUtil.encode(
                message,
                unicode ? CharsetUtil.CHARSET_UCS_2 : CharsetUtil.CHARSET_GSM
                                            );

        parts.add(new SmsFragment(
                msgBytes,
                false,
                source,
                destination,
                dataCoding
        ));

        return parts;
    }

    // --------------------------------------------------------
    // MULTIPART BUILDER
    // --------------------------------------------------------

    static List<SmsFragment> buildMultipart(
            String message,
            Address source,
            Address destination,
            int ref,
            boolean unicode) {

        List<SmsFragment> parts = new ArrayList<>();

        byte dataCoding = unicode ? (byte) 0x08 : (byte) 0x00;

        byte[] msgBytes = CharsetUtil.encode(
                message,
                unicode ? CharsetUtil.CHARSET_UCS_2 : CharsetUtil.CHARSET_GSM
                                            );

        int singleLimit = unicode ? 140 : 160;
        int multiLimit = unicode ? 134 : 153;

        if (msgBytes.length <= singleLimit) {
            parts.add(new SmsFragment(
                    msgBytes,
                    false,
                    source,
                    destination,
                    dataCoding
            ));
            return parts;
        }

        int totalParts = (int) Math.ceil((double) msgBytes.length / multiLimit);

        for (int part = 1; part <= totalParts; part++) {
            int start = (part - 1) * multiLimit;
            int len = Math.min(multiLimit, msgBytes.length - start);

            byte[] body = new byte[len];
            System.arraycopy(msgBytes, start, body, 0, len);

            byte[] udh = createUdh8Bit(ref, totalParts, part);

            ByteBuffer buffer = ByteBuffer.allocate(udh.length + body.length);
            buffer.put(udh);
            buffer.put(body);

            parts.add(new SmsFragment(
                    buffer.array(),
                    true,
                    source,
                    destination,
                    dataCoding
            ));
        }

        return parts;
    }

    private static byte[] createUdh8Bit(int ref, int total, int seq) {
        return new byte[]{
                0x05,       // UDH length excluding this byte
                0x00,       // IEI: concatenated short message, 8-bit ref
                0x03,       // IE length
                (byte) (ref & 0xFF),
                (byte) total,
                (byte) seq
        };
    }

    // --------------------------------------------------------
    // METRICS
    // --------------------------------------------------------

    static void startMetrics() {
        ScheduledExecutorService metrics = Executors.newScheduledThreadPool(1);
        AtomicLong lastSent = new AtomicLong();

        metrics.scheduleAtFixedRate(() -> {
            long current = sent.get();
            long delta = current - lastSent.getAndSet(current);

            log.error(
                    "TPS={} sentDelta={} produced={} sentTotal={} success={} failed={} queue={} singleMsg={} multipartMsg={} dlrReceivedTotal={}",
                    delta / 5,
                    delta,
                    produced.get(),
                    sent.get(),
                    success.get(),
                    failed.get(),
                    queue.size(),
                    generatedSingleMessages.get(),
                    generatedMultipartMessages.get(),
                    dlrReceived.get()
                     );
        }, 5, 5, TimeUnit.SECONDS);
    }

    // --------------------------------------------------------
    // DATA MODEL
    // --------------------------------------------------------

    static final class SmsFragment {
        final byte[] payload;
        final boolean hasUdh;
        final Address source;
        final Address destination;
        final byte dataCoding;

        SmsFragment(byte[] payload,
                    boolean hasUdh,
                    Address source,
                    Address destination,
                    byte dataCoding) {
            this.payload = payload;
            this.hasUdh = hasUdh;
            this.source = source;
            this.destination = destination;
            this.dataCoding = dataCoding;
        }
    }

    // --------------------------------------------------------
    // SESSION HANDLER
    // --------------------------------------------------------

    static class Handler extends DefaultSmppSessionHandler {
        @Override
        public void fireChannelUnexpectedlyClosed() {
            log.error("SMPP session closed unexpectedly");
        }
        @Override
        public PduResponse firePduRequestReceived(PduRequest request) {

            if (request instanceof DeliverSm) {
                dlrReceived.incrementAndGet();

                DeliverSm deliverSm = (DeliverSm) request;

                log.debug("DLR received: {}", new String(deliverSm.getShortMessage()));
            }

            return request.createResponse();
        }
    }

    // --------------------------------------------------------
    // RANDOM ADDRESS GENERATORS
    // --------------------------------------------------------

    private static Address randomDestination() {
        long number = 910000000000L + ThreadLocalRandom.current().nextLong(1_000_000_000L);
        return new Address((byte) 1, (byte) 1, String.valueOf(number));
    }

    private static Address randomSource() {
        String header = HEADERS[ThreadLocalRandom.current().nextInt(HEADERS.length)];
        return new Address((byte) 1, (byte) 1, header);
    }

    // --------------------------------------------------------
    // ENV HELPERS
    // --------------------------------------------------------

    private static String getEnv(String key, String def) {
        String v = System.getenv(key);
        return (v == null || v.isBlank()) ? def : v.trim();
    }

    private static int getIntEnv(String key, int def) {
        try {
            return Integer.parseInt(getEnv(key, String.valueOf(def)));
        } catch (Exception e) {
            return def;
        }
    }

    private static long getLongEnv(String key, long def) {
        try {
            return Long.parseLong(getEnv(key, String.valueOf(def)));
        } catch (Exception e) {
            return def;
        }
    }

    private static boolean getBooleanEnv(String key, boolean def) {
        String v = System.getenv(key);
        if (v == null || v.isBlank()) {
            return def;
        }
        return "true".equalsIgnoreCase(v.trim())
                || "1".equals(v.trim())
                || "yes".equalsIgnoreCase(v.trim());
    }
}