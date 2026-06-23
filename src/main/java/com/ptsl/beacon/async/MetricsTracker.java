package com.ptsl.beacon.async;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricsTracker {
    private static final Logger log = LoggerFactory.getLogger(MetricsTracker.class);

    public final AtomicLong produced = new AtomicLong();
    public final AtomicLong sent = new AtomicLong();
    public final AtomicLong success = new AtomicLong();
    public final AtomicLong failed = new AtomicLong();
    public final AtomicLong dlrReceived = new AtomicLong();

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private final AtomicLong lastSent = new AtomicLong();
    private final SmppSessionManager sessionManager;
    private final LoadTestConfig config;
    private final java.util.concurrent.BlockingQueue<?> queue;

    public MetricsTracker(LoadTestConfig config, SmppSessionManager sessionManager, java.util.concurrent.BlockingQueue<?> queue) {
        this.config = config;
        this.sessionManager = sessionManager;
        this.queue = queue;
    }

    public void startReporting() {
        scheduler.scheduleAtFixedRate(() -> {
            long currentSent = sent.get();
            long delta = currentSent - lastSent.getAndSet(currentSent);

            int liveSessions = sessionManager.getLiveSessionCount();

            log.error(
                    "TPS={} sentDelta={} produced={} sentTotal={} success={} failed={} queue={} dlrReceivedTotal={} liveSessions={}/{}",
                    delta / 5,
                    delta,
                    produced.get(),
                    sent.get(),
                    success.get(),
                    failed.get(),
                    queue.size(),
                    dlrReceived.get(),
                    liveSessions,
                    config.sessions
            );

            // Auto-exit if all maxMessages have been fully processed (either success or failed)
            long totalProcessed = success.get() + failed.get();
            if (totalProcessed >= config.maxMessages) {
                log.error("Load test completed successfully. Processed {}/{} messages (Success: {}, Failed: {}). Exiting.", 
                        totalProcessed, config.maxMessages, success.get(), failed.get());
                System.exit(0);
            }
        }, 5, 5, TimeUnit.SECONDS);
    }

    public void stopReporting() {
        scheduler.shutdown();
    }
}
