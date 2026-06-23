package com.ptsl.beacon.async;

import com.cloudhopper.smpp.SmppConstants;
import com.cloudhopper.smpp.SmppSession;
import com.cloudhopper.smpp.pdu.SubmitSm;
import com.cloudhopper.smpp.tlv.Tlv;
import com.cloudhopper.smpp.type.SmppInvalidArgumentException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageSender {
    private static final Logger log = LoggerFactory.getLogger(MessageSender.class);

    private final LoadTestConfig config;
    private final SmppSessionManager sessionManager;
    private final BlockingQueue<SmsFragment> queue;
    private final MetricsTracker metricsTracker;
    private ExecutorService workers;

    public MessageSender(LoadTestConfig config, SmppSessionManager sessionManager, BlockingQueue<SmsFragment> queue, MetricsTracker metricsTracker) {
        this.config = config;
        this.sessionManager = sessionManager;
        this.queue = queue;
        this.metricsTracker = metricsTracker;
    }

    public void start() {
        this.workers = Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("smpp-async-worker-", 0).factory());

        for (int i = 0; i < config.workerThreads; i++) {
            final int sessionIndex = i % config.sessions;
            workers.submit(() -> {
                while (true) {
                    SmsFragment fragment = null;
                    try {
                        fragment = queue.take();

                        SmppSession session = sessionManager.getSession(sessionIndex);

                        if (session == null || !session.isBound()) {
                            // If this session is down, re-queue the fragment and back off
                            queue.put(fragment);
                            Thread.sleep(200);
                            continue;
                        }

                        SubmitSm sm = createSubmitSm(fragment);
                        
                        // Send asynchronously (synchronous = false)
                        session.sendRequestPdu(sm, 10000, false);

                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    } catch (Exception e) {
                        metricsTracker.failed.incrementAndGet();
                        log.debug("Async submit failed", e);
                        try {
                            Thread.sleep(50);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                    }
                }
            });
        }
    }

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

        // Add DLT Custom TLVs
        sm.addOptionalParameter(new Tlv((short) 0x1400, "110100001403".getBytes()));
        sm.addOptionalParameter(new Tlv((short) 0x1401, "1107174074670670034".getBytes()));

        return sm;
    }

    public void shutdown() {
        if (workers != null) {
            workers.shutdownNow();
        }
    }
}
