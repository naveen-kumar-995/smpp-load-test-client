package com.ptsl.beacon.async;

import com.cloudhopper.smpp.SmppBindType;
import java.util.Locale;

public class LoadTestConfig {
    public final int sessions;
    public final int windowSize;
    public final int workerThreads;
    public final int queueSize;
    public final String host;
    public final int port;
    public final String systemId;
    public final String password;
    public final long maxMessages;
    public final String messageMode;
    public final int singlePercent;
    public final boolean unicode;
    public final String singleMessage;
    public final String multipartMessage;
    public final SmppBindType bindType;

    public LoadTestConfig() {
        this.sessions = getIntEnv("SESSIONS", 5);
        this.windowSize = getIntEnv("WINDOW_SIZE", 1000);
        this.workerThreads = getIntEnv("WORKER_THREADS", 50);
        this.queueSize = getIntEnv("QUEUE_SIZE", 100000);
        this.host = getEnv("SMSC_HOST", "127.0.0.1");
        this.port = getIntEnv("SMSC_PORT", 2775);
        this.systemId = getEnv("SMSC_SYSTEM_ID", "test");
        this.password = getEnv("SMSC_PASSWORD", "test");
        this.maxMessages = getLongEnv("MAX_MESSAGES", 1000000L);
        this.messageMode = getEnv("MESSAGE_MODE", "multipart").trim().toLowerCase(Locale.ROOT);
        this.singlePercent = Math.max(0, Math.min(100, getIntEnv("SINGLE_PERCENT", 50)));
        this.unicode = getBooleanEnv("UNICODE", false);
        this.singleMessage = getEnv("SINGLE_MESSAGE",
                "Canara Bank alert: INR 1000 credited to your account. Ref {#var#}");
        this.multipartMessage = getEnv("MULTIPART_MESSAGE",
                "An amount of {#var#} has been debited to {#var#} on {#var#} towards {#var#} "
                        + "fvg Benf {#var#}, IFSC {#var#}, Benf A/c {#var#}, UTR {#var#}. "
                        + "Total Avail. Bal INR {#var#} -Canara Bank");
        this.bindType = parseBindType(getEnv("SMPP_BIND_TYPE", "TRX"));
    }

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

    private static SmppBindType parseBindType(String value) {
        if (value != null) {
            if ("TX".equalsIgnoreCase(value)) {
                return SmppBindType.TRANSMITTER;
            } else if ("RX".equalsIgnoreCase(value)) {
                return SmppBindType.RECEIVER;
            }
        }
        return SmppBindType.TRANSCEIVER;
    }
}
