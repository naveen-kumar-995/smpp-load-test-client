package com.ptsl.beacon;


import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public final class DlrLoadTest {

    private static final String DEFAULT_URL = "http://localhost:8080/dlr";
    private static final int DEFAULT_CONCURRENCY = 1000;
    private static final int DEFAULT_DURATION_SECONDS = 60;
    private static final int DEFAULT_CONNECT_TIMEOUT_SECONDS = 3;
    private static final int DEFAULT_REQUEST_TIMEOUT_SECONDS = 5;

    private final String baseUrl;
    private final int concurrency;
    private final int durationSeconds;

    private final AtomicBoolean running = new AtomicBoolean(true);

    private final AtomicLong totalRequests = new AtomicLong();
    private final AtomicLong successRequests = new AtomicLong();
    private final AtomicLong failedRequests = new AtomicLong();
    private final AtomicLong non200Requests = new AtomicLong();

    private final AtomicLong totalLatencyMicros = new AtomicLong();
    private final AtomicLong latencyCount = new AtomicLong();
    private final AtomicLong minLatencyMicros = new AtomicLong(Long.MAX_VALUE);
    private final AtomicLong maxLatencyMicros = new AtomicLong(0);

    private final AtomicLong secondRequests = new AtomicLong();
    private final AtomicLong secondSuccess = new AtomicLong();
    private final AtomicLong secondFailures = new AtomicLong();
    private final AtomicLong secondNon200 = new AtomicLong();

    private final HttpClient client;

    public DlrLoadTest(String baseUrl, int concurrency, int durationSeconds) {
        this.baseUrl = baseUrl;
        this.concurrency = concurrency;
        this.durationSeconds = durationSeconds;
        this.client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(DEFAULT_CONNECT_TIMEOUT_SECONDS))
                .version(HttpClient.Version.HTTP_1_1)
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .build();
    }

    public static void main(String[] args) throws Exception {
        String url = getEnv("dlr_url", DEFAULT_URL);
        int concurrency = getIntEnv("dlr_concurrency" , DEFAULT_CONCURRENCY);
        int duration = getIntEnv("dlr_duration_sec", DEFAULT_DURATION_SECONDS);
        DlrLoadTest test = new DlrLoadTest(url, concurrency, duration);
        test.run();
    }

    public void run() throws Exception {
        System.out.println("Starting saturation load test");
        System.out.println("URL         : " + baseUrl);
        System.out.println("Concurrency : " + concurrency);
        System.out.println("Duration    : " + durationSeconds + " sec");
        System.out.println();

        long startNanos = System.nanoTime();

        Thread statsThread = Thread.ofVirtual().name("stats-printer").start(this::printStatsLoop);

        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < concurrency; i++) {
                final int workerId = i;
                executor.submit(() -> workerLoop(workerId));
            }

            Thread.sleep(durationSeconds * 1000L);
            running.set(false);
        }

        statsThread.interrupt();
        statsThread.join(2000);

        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
        printFinalSummary(elapsedMillis);
    }

    private void workerLoop(int workerId) {
        while (running.get() && !Thread.currentThread().isInterrupted()) {
            sendOne(workerId);
        }
    }

    private void sendOne(int workerId) {
        String msgId = UUID.randomUUID().toString();
        String separator = baseUrl.contains("?") ? "&" : "?";
        String url = baseUrl + separator + "msgid=" + msgId;

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .GET()
                .timeout(Duration.ofSeconds(DEFAULT_REQUEST_TIMEOUT_SECONDS))
                .build();

        long start = System.nanoTime();

        try {
            HttpResponse<Void> response = client.send(request, HttpResponse.BodyHandlers.discarding());

            long latencyMicros = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - start);
            recordLatency(latencyMicros);

            totalRequests.incrementAndGet();
            secondRequests.incrementAndGet();

            if (response.statusCode() == 200) {
                successRequests.incrementAndGet();
                secondSuccess.incrementAndGet();
            } else {
                non200Requests.incrementAndGet();
                secondNon200.incrementAndGet();
            }

        } catch (Exception e) {
            long latencyMicros = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - start);
            recordLatency(latencyMicros);

            totalRequests.incrementAndGet();
            secondRequests.incrementAndGet();
            failedRequests.incrementAndGet();
            secondFailures.incrementAndGet();
        }
    }

    private void recordLatency(long latencyMicros) {
        latencyCount.incrementAndGet();
        totalLatencyMicros.addAndGet(latencyMicros);
        updateMin(minLatencyMicros, latencyMicros);
        updateMax(maxLatencyMicros, latencyMicros);
    }

    private void updateMin(AtomicLong target, long value) {
        long current;
        do {
            current = target.get();
            if (value >= current) {
                return;
            }
        } while (!target.compareAndSet(current, value));
    }

    private void updateMax(AtomicLong target, long value) {
        long current;
        do {
            current = target.get();
            if (value <= current) {
                return;
            }
        } while (!target.compareAndSet(current, value));
    }

    private void printStatsLoop() {
        try {
            while (!Thread.currentThread().isInterrupted()) {
                Thread.sleep(1000);

                long req = secondRequests.getAndSet(0);
                long ok = secondSuccess.getAndSet(0);
                long fail = secondFailures.getAndSet(0);
                long non200 = secondNon200.getAndSet(0);

                System.out.printf(
                        "[1s] achieved_tps=%d success=%d non200=%d failures=%d%n",
                        req, ok, non200, fail
                                 );
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void printFinalSummary(long elapsedMillis) {
        long total = totalRequests.get();
        long success = successRequests.get();
        long failures = failedRequests.get();
        long non200 = non200Requests.get();

        double avgLatencyMs = latencyCount.get() == 0
                ? 0.0
                : (totalLatencyMicros.get() / (double) latencyCount.get()) / 1000.0;

        double minLatencyMs = minLatencyMicros.get() == Long.MAX_VALUE
                ? 0.0
                : minLatencyMicros.get() / 1000.0;

        double maxLatencyMs = maxLatencyMicros.get() / 1000.0;

        double achievedTps = elapsedMillis == 0
                ? 0.0
                : total / (elapsedMillis / 1000.0);

        System.out.println();
        System.out.println("========== FINAL SUMMARY ==========");
        System.out.println("Elapsed Time (ms) : " + elapsedMillis);
        System.out.println("Total Requests    : " + total);
        System.out.println("Success           : " + success);
        System.out.println("Non-200           : " + non200);
        System.out.println("Failures          : " + failures);
        System.out.printf("Achieved TPS      : %.2f%n", achievedTps);
        System.out.printf("Avg Latency (ms)  : %.3f%n", avgLatencyMs);
        System.out.printf("Min Latency (ms)  : %.3f%n", minLatencyMs);
        System.out.printf("Max Latency (ms)  : %.3f%n", maxLatencyMs);
    }

    private static String getArg(String[] args, String key, String defaultValue) {
        for (int i = 0; i < args.length - 1; i++) {
            if (key.equals(args[i])) {
                return args[i + 1];
            }
        }
        return defaultValue;
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