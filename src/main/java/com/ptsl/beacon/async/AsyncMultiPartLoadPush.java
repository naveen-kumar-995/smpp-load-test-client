package com.ptsl.beacon.async;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AsyncMultiPartLoadPush {
    private static final Logger log = LoggerFactory.getLogger(AsyncMultiPartLoadPush.class);

    public static void main(String[] args) {
        log.error("Starting Asynchronous SMPP Load Client...");

        LoadTestConfig config = new LoadTestConfig();
        printConfig(config);

        BlockingQueue<SmsFragment> queue = new ArrayBlockingQueue<>(config.queueSize);
        SmppSessionManager sessionManager = new SmppSessionManager(config);
        MetricsTracker metricsTracker = new MetricsTracker(config, sessionManager, queue);

        // Bind circular dependencies
        sessionManager.setMetricsTracker(metricsTracker);

        try {
            // 1. Initialize sessions
            sessionManager.initialize();
        } catch (Exception e) {
            log.error("Failed to initialize SMPP sessions. Exiting.", e);
            System.exit(1);
        }

        // 2. Start services
        sessionManager.startEnquireLink();
        sessionManager.startWatchdog();
        metricsTracker.startReporting();

        MessageGenerator generator = new MessageGenerator(config, metricsTracker, queue);
        MessageSender sender = new MessageSender(config, sessionManager, queue, metricsTracker);

        generator.start();
        sender.start();

        // 3. Register graceful shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.error("Shutdown hook triggered. Cleaning up resources...");
            sender.shutdown();
            sessionManager.shutdown();
            metricsTracker.stopReporting();
            log.error("Graceful shutdown complete.");
        }));

        log.error("All services started successfully. Load is running.");
    }

    private static void printConfig(LoadTestConfig config) {
        log.error("========== SMPP ASYNC LOAD CONFIG ==========");
        log.error("Host={}", config.host);
        log.error("Port={}", config.port);
        log.error("SystemID={}", config.systemId);
        log.error("Sessions={}", config.sessions);
        log.error("Window={}", config.windowSize);
        log.error("Workers={}", config.workerThreads);
        log.error("QueueSize={}", config.queueSize);
        log.error("MaxMessages={}", config.maxMessages);
        log.error("MessageMode={}", config.messageMode);
        log.error("SinglePercent={}", config.singlePercent);
        log.error("Unicode={}", config.unicode);
        log.error("BindType={}", config.bindType);
        log.error("TargetTPS={}", config.targetTps);
        log.error("============================================");
    }
}
