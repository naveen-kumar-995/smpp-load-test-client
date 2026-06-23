package com.ptsl.beacon.async;

import com.cloudhopper.smpp.SmppSession;
import com.cloudhopper.smpp.SmppSessionConfiguration;
import com.cloudhopper.smpp.impl.DefaultSmppClient;
import com.cloudhopper.smpp.pdu.EnquireLink;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SmppSessionManager {
    private static final Logger log = LoggerFactory.getLogger(SmppSessionManager.class);

    private final LoadTestConfig config;
    private final AtomicReferenceArray<SmppSession> sessions;
    private DefaultSmppClient client;
    private ScheduledExecutorService watchdog;
    private ScheduledExecutorService enquireLinkScheduler;
    private MetricsTracker metricsTracker;

    public SmppSessionManager(LoadTestConfig config) {
        this.config = config;
        this.sessions = new AtomicReferenceArray<>(config.sessions);
    }

    public void setMetricsTracker(MetricsTracker metricsTracker) {
        this.metricsTracker = metricsTracker;
    }

    public void initialize() throws Exception {
        ExecutorService smppExecutor = Executors.newCachedThreadPool();
        this.client = new DefaultSmppClient(smppExecutor, config.sessions);

        for (int i = 0; i < config.sessions; i++) {
            sessions.set(i, bindSession(i));
        }
        log.error("All {} sessions connected initially.", config.sessions);
    }

    public SmppSession getSession(int index) {
        return sessions.get(index);
    }

    public int getLiveSessionCount() {
        int count = 0;
        for (int i = 0; i < config.sessions; i++) {
            SmppSession s = sessions.get(i);
            if (s != null && s.isBound()) {
                count++;
            }
        }
        return count;
    }

    private SmppSession bindSession(int index) throws Exception {
        SmppSessionConfiguration cfg = createSessionConfig(index);
        String name = "session-" + index;
        return client.bind(cfg, new SmppResponseHandler(name, metricsTracker));
    }

    private SmppSessionConfiguration createSessionConfig(int index) {
        SmppSessionConfiguration cfg = new SmppSessionConfiguration();
        cfg.setName("session-" + index);
        cfg.setType(config.bindType);
        cfg.setHost(config.host);
        cfg.setPort(config.port);
        cfg.setSystemId(config.systemId);
        cfg.setPassword(config.password);
        cfg.setWindowSize(config.windowSize);
        cfg.setConnectTimeout(10000);
        cfg.setRequestExpiryTimeout(10000);
        cfg.setWindowMonitorInterval(15000);
        return cfg;
    }

    public void startWatchdog() {
        this.watchdog = Executors.newScheduledThreadPool(1);
        watchdog.scheduleAtFixedRate(() -> {
            for (int i = 0; i < config.sessions; i++) {
                try {
                    SmppSession session = sessions.get(i);
                    if (session == null || !session.isBound()) {
                        log.error("Watchdog: Session {} is disconnected. Reconnecting...", i);
                        if (session != null) {
                            try { session.close(); } catch (Exception ignored) {}
                            try { session.destroy(); } catch (Exception ignored) {}
                        }
                        sessions.set(i, bindSession(i));
                        log.error("Watchdog: Session {} successfully reconnected!", i);
                    }
                } catch (Exception e) {
                    log.error("Watchdog: Failed to reconnect session {}: {}", i, e.getMessage());
                }
            }
        }, 10, 5, TimeUnit.SECONDS);
    }

    public void startEnquireLink() {
        this.enquireLinkScheduler = Executors.newScheduledThreadPool(1);
        enquireLinkScheduler.scheduleAtFixedRate(() -> {
            for (int i = 0; i < config.sessions; i++) {
                try {
                    SmppSession session = sessions.get(i);
                    if (session != null && session.isBound()) {
                        session.enquireLink(new EnquireLink(), 5000);
                        log.debug("EnquireLink sent for session {}", i);
                    }
                } catch (Exception e) {
                    log.error("EnquireLink failed for session {}: {}", i, e.getMessage());
                }
            }
        }, 30, 30, TimeUnit.SECONDS);
    }

    public void shutdown() {
        log.error("Shutting down SMPP sessions...");
        if (watchdog != null) {
            watchdog.shutdownNow();
        }
        if (enquireLinkScheduler != null) {
            enquireLinkScheduler.shutdownNow();
        }
        for (int i = 0; i < config.sessions; i++) {
            try {
                SmppSession session = sessions.get(i);
                if (session != null && session.isBound()) {
                    session.unbind(5000);
                    session.close();
                    session.destroy();
                    log.error("Session {} unbound cleanly.", i);
                }
            } catch (Exception e) {
                log.error("Error closing session {}: {}", i, e.getMessage());
            }
        }
        if (client != null) {
            client.destroy();
        }
    }
}
