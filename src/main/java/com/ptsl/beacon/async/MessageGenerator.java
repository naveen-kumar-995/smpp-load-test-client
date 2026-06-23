package com.ptsl.beacon.async;

import com.cloudhopper.commons.charset.CharsetUtil;
import com.cloudhopper.smpp.type.Address;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageGenerator {
    private static final Logger log = LoggerFactory.getLogger(MessageGenerator.class);

    private static final String[] HEADERS = {
            "CANBNK", "HDFCBK", "ICICIB", "AXISBK", "SBIOTP",
            "PAYTMB", "AMAZON", "FLIPKT", "MYBANK", "UPIOTP"
    };

    private final LoadTestConfig config;
    private final MetricsTracker metricsTracker;
    private final BlockingQueue<SmsFragment> queue;
    private final AtomicInteger refGen = new AtomicInteger();
    private Thread generatorThread;

    public MessageGenerator(LoadTestConfig config, MetricsTracker metricsTracker, BlockingQueue<SmsFragment> queue) {
        this.config = config;
        this.metricsTracker = metricsTracker;
        this.queue = queue;
    }

    public void start() {
        generatorThread = new Thread(() -> {
            long targetTps = config.targetTps;
            long secondStart = System.currentTimeMillis();
            long producedInSecond = 0;

            while (metricsTracker.produced.get() < config.maxMessages) {
                // Rate Limiting Throttler
                if (targetTps > 0) {
                    long now = System.currentTimeMillis();
                    if (now - secondStart >= 1000) {
                        secondStart = now;
                        producedInSecond = 0;
                    } else if (producedInSecond >= targetTps) {
                        long sleepTime = 1000 - (now - secondStart);
                        if (sleepTime > 0) {
                            try {
                                Thread.sleep(sleepTime);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                return;
                            }
                        }
                        secondStart = System.currentTimeMillis();
                        producedInSecond = 0;
                    }
                }

                Address src = randomSource();
                Address dst = randomDestination();

                List<SmsFragment> fragments = createNextMessage(src, dst);

                for (SmsFragment fragment : fragments) {
                    long next = metricsTracker.produced.incrementAndGet();

                    if (next > config.maxMessages) {
                        return;
                    }

                    producedInSecond++;

                    try {
                        queue.put(fragment);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }
            log.error("Generator finished producing PDUs. Total produced={}", metricsTracker.produced.get());
        });

        generatorThread.setName("message-generator");
        generatorThread.setDaemon(true);
        generatorThread.start();
    }

    private List<SmsFragment> createNextMessage(Address source, Address destination) {
        boolean sendSingle = shouldSendSingle();

        if (sendSingle) {
            return buildSinglePart(config.singleMessage, source, destination, config.unicode);
        } else {
            int ref = refGen.incrementAndGet() & 0xFF;
            return buildMultipart(config.multipartMessage, source, destination, ref, config.unicode);
        }
    }

    private boolean shouldSendSingle() {
        return switch (config.messageMode) {
            case "single" -> true;
            case "multipart" -> false;
            case "mixed" -> ThreadLocalRandom.current().nextInt(100) < config.singlePercent;
            default -> false;
        };
    }

    private static Address randomDestination() {
        long number = 910000000000L + ThreadLocalRandom.current().nextLong(1_000_000_000L);
        return new Address((byte) 1, (byte) 1, String.valueOf(number));
    }

    private static Address randomSource() {
        String header = HEADERS[ThreadLocalRandom.current().nextInt(HEADERS.length)];
        return new Address((byte) 1, (byte) 1, header);
    }

    private static List<SmsFragment> buildSinglePart(
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

        parts.add(new SmsFragment(msgBytes, false, source, destination, dataCoding));
        return parts;
    }

    private static List<SmsFragment> buildMultipart(
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
            parts.add(new SmsFragment(msgBytes, false, source, destination, dataCoding));
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

            parts.add(new SmsFragment(buffer.array(), true, source, destination, dataCoding));
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
}
