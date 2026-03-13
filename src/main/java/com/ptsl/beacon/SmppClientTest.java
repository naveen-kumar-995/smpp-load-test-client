package com.ptsl.beacon;


import com.cloudhopper.smpp.SmppConstants;
import com.cloudhopper.smpp.SmppSession;
import com.cloudhopper.smpp.SmppSessionConfiguration;
import com.cloudhopper.smpp.SmppBindType;
import com.cloudhopper.smpp.impl.DefaultSmppClient;
import com.cloudhopper.smpp.impl.DefaultSmppSessionHandler;
import com.cloudhopper.smpp.pdu.EnquireLink;
import com.cloudhopper.smpp.pdu.SubmitSm;
import com.cloudhopper.smpp.pdu.SubmitSmResp;
import com.cloudhopper.smpp.type.Address;
import com.cloudhopper.commons.charset.CharsetUtil;
import com.cloudhopper.smpp.tlv.Tlv;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A testing utility to simulate multiple SMPP clients connecting to the SMSC.
 * It demonstrates how to configure sessions, bind, send pings (EnquireLink),
 * and submit standard/Unicode messages independently across concurrent threads.
 */
public class SmppClientTest {

    // --- Configuration Constants ---
    private static final int NUMBER_OF_SESSIONS = 1;
    private static final int MESSAGES_PER_SESSION = 1;
    private static final boolean SEND_MULTIPART = true;
    private static final String SMSC_HOST = "172.25.4.191";
    private static final int SMSC_PORT = 2775;
    private static final String SYSTEM_ID = "naveen_test_01";
    private static final String PASSWORD = "pass123";


    public static void main(String[] args) throws Exception {
        System.out.println("Starting Test Runner...");

        // 1. Initialize Thread Pools required by Cloudhopper SMPP
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newCachedThreadPool();
        ScheduledThreadPoolExecutor monitorExecutor = createMonitorExecutor();

        // 2. Create the SMPP Client which manages all sessions
        DefaultSmppClient client = new DefaultSmppClient(executor, NUMBER_OF_SESSIONS, monitorExecutor);

        System.out.println("Spawning " + NUMBER_OF_SESSIONS + " concurrent SMPP sessions...");
        CountDownLatch latch = new CountDownLatch(NUMBER_OF_SESSIONS);

        // 3. Start sessions concurrently
        for (int i = 0; i < NUMBER_OF_SESSIONS; i++) {
            final int sessionId = i;
            new Thread(() -> {
                try {
                    SmppSessionRunner runner = new SmppSessionRunner(client, sessionId);
                    runner.execute();
                } finally {
                    latch.countDown();
                }
            }).start();
        }
        // 4. Wait for all background sessions to complete
        latch.await();
        System.out.println("\nAll sessions have completed their tasks!");

        System.out.println("Sleeping for 8 seconds to allow the Server's Reaper to pick up the incomplete message...");
        try { Thread.sleep(8000); } catch (InterruptedException e) {}

        // 5. Unbind and close all sessions

        // 6. Cleanup Resources
        client.destroy();
        executor.shutdownNow();
        monitorExecutor.shutdownNow();
        System.out.println("Client threads shut down. Exiting.");
    }

    /**
     * Creates a ScheduledThreadPoolExecutor for monitoring session window sizes and request expirations.
     */
    private static ScheduledThreadPoolExecutor createMonitorExecutor() {
        return (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(1, new ThreadFactory() {
            private final AtomicInteger sequence = new AtomicInteger(0);
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setName("SmppClientMonitorPool-" + sequence.getAndIncrement());
                return t;
            }
        });
    }

    /**
     * Encapsulates the lifecycle of a single SMPP session.
     */
    private static class SmppSessionRunner {
        private final DefaultSmppClient smppClient;
        private final int sessionId;

        public SmppSessionRunner(DefaultSmppClient smppClient, int sessionId) {
            this.smppClient = smppClient;
            this.sessionId = sessionId;
        }

        /**
         * Runs the full lifecycle: Configure -> Bind -> Ping -> Send Messages -> Unbind.
         */
        public void execute() {
            SmppSessionConfiguration config = createSessionConfiguration();
            SmppSession session = null;

            try {
                // 1. Bind to SMSC
                session = smppClient.bind(config, new ClientSessionHandler());
                log("Successfully bound to SMSC!");

                // 2. Ping the server
                sendEnquireLink(session);

                // 3. Submit Messages
                sendMessages(session);

                // 4. Wait for async responses to return from SMSC
                Thread.sleep(1000);

                // 5. Unbind Gracefully
                unbindSession(session);

            } catch (Exception e) {
                logError("encountered an error: " + e.getMessage());
            } finally {
                // 6. Ensure connection is closed and resources destroyed
                if (session != null) {
                    session.close();
                    session.destroy();
                    log("Connection permanently closed.");
                }
            }
        }

        private SmppSessionConfiguration createSessionConfiguration() {
            SmppSessionConfiguration config = new SmppSessionConfiguration();
            config.setWindowSize(1);
            config.setName("Tester.Session." + sessionId);
            config.setType(SmppBindType.TRANSCEIVER);
            config.setHost(SMSC_HOST);
            config.setPort(SMSC_PORT);
            config.setConnectTimeout(10000);
            config.setSystemId(SYSTEM_ID);
            config.setPassword(PASSWORD);
            config.getLoggingOptions().setLogBytes(false); // Enable if you want deep byte-level debugging
            config.getLoggingOptions().setLogPdu(true);
            return config;
        }

        private void sendEnquireLink(SmppSession session) throws Exception {
            EnquireLink ping = new EnquireLink();
            session.enquireLink(ping, 10000);
            log("EnquireLink (Ping) successful!");
        }

        private void sendMessages(SmppSession session) throws Exception {
            for (int i = 1; i <= MESSAGES_PER_SESSION; i++) {
                if (SEND_MULTIPART) {
                    sendMultipartTestScenarios(session, i);
                } else {
                    sendSinglePartMessage(session, i, true); // Plain GSM
//                    sendSinglePartMessage(session, i, true);  // Unicode UCS2
                }
            }
        }

        private void sendSinglePartMessage(SmppSession session, int msgIndex, boolean isUnicode) throws Exception {
            String msgText = isUnicode
                    ? "Unicode Test \u2728 (Session " + sessionId + ", Msg " + msgIndex + ")"
                    : "Plain Test (Session " + sessionId + ", Msg " + msgIndex + ")";
            String msgTxt ;
            if(isUnicode)
            {
                msgTxt   = "जन्मदिन की हार्दिक शुभकामनाएं केनरा बैंक Happy Birthday Canara Bank";
            }else {
                msgTxt = "Thanks for showing interest on CANARA BUDGET LOAN of Canara Bank. Your Regenerated MPIN is !@#$%^&*().Pls enter the same to continue the online application.";
            }
            byte[] textBytes = CharsetUtil.encode(msgTxt, isUnicode ? CharsetUtil.CHARSET_UCS_2 : CharsetUtil.CHARSET_GSM);

            SubmitSm submitSm = createSubmitSm(isUnicode, false, textBytes);
            submitAndWait(session, submitSm, String.format("Single Part (%s)", isUnicode ? "Unicode" : "Plain"));
        }

        private void sendMultipartTestScenarios(SmppSession session, int refNumStart) throws Exception {
//            // SCENARIO 3: Missing Fragment (Zombie Reaper Test)
//            log("--- SCENARIO 3: Sending Incomplete Message (Missing Part 2) - Plain GSM ---");
//            sendMultipart(session, 30 + refNumStart, 3, new int[]{1, 3}, false);
//
//            log("--- SCENARIO 4: Sending Incomplete Message (Missing Part 2) - Unicode ---");
//            sendMultipart(session, 40 + refNumStart, 3, new int[]{1, 3}, true);

            // SCENARIO 5: Auto-Splitting Full Text
            log("--- SCENARIO 5: Auto-Splitting Long Message - Plain GSM ---");
//            String longMessage = "".repeat(3);


            String longMessage = "An amount of !@#$%^ has been debited to *()_- on 12345 towards 12345 fvg Benf 12345, IFSC 12345, Benf A/c 12345, UTR 12345. Total Avail. Bal INR 12345 -Canara Bank";

//            sendAutoMultipart(session, 50 + refNumStart, longMessage, false);

            String unicodelong = "மார்ச் 4 2025 வருகின்ற செவ்வாய் கிழமை அன்று ங்கரன்கோவில் நீதிமன்ற கட்டிட வளாகத்தில் நடைபெறவுள்ள லோக் அதாலத் தற்போது உள்ளூர் விடுமுறை காரணத்தினால் 03.03.2025 திங்கள் கிழமை அன்று மாற்றப்பட்டுள்ளது என்பதை தெரிவித்துக் கொள்கிறோம். கனரா வங்கி மண்டல அலுவலகம் தூத்துக்குடி";
            sendAutoMultipart(session, 60 + refNumStart, unicodelong, true);

            // SCENARIO 6: Auto-Splitting Full Text (Unicode)
//            log("--- SCENARIO 6: Auto-Splitting Long Message - Unicode ---");
//            String longUnicodeMessage = "✨ அன்புள்ள வாடிக்கையாளரே, இது ஒரு மிக நீண்ட தமிழ் குறுஞ்செய்தி. இதைப் படிப்பதன் மூலம் நீங்கள் தானியங்கி செய்தி பிரிப்பு முறையை வெற்றிகரமாக சோதிக்கிறீர்கள். ".repeat(3);
//            sendAutoMultipart(session, 60 + refNumStart, longUnicodeMessage, true);
        }


        private void sendMultipart(SmppSession session, int refNum, int totalParts, int[] sendSequence, boolean isUnicode) throws Exception {
            for (int partNum : sendSequence) {
                byte[] udh = buildUdh(refNum, totalParts, partNum);

                String textChunk = isUnicode
                        ? String.format("[\u2728 Part %d of %d for Ref %d] ", partNum, totalParts, refNum) + "உங்களுக்கு ₹10000 பரிசு கிடைத்துள்ளது. ".repeat(3)
                        : String.format("[Part %d of %d for Ref %d] ", partNum, totalParts, refNum) + "Your OTP is 1234. ".repeat(2);

                byte[] textBytes = CharsetUtil.encode(textChunk, isUnicode ? CharsetUtil.CHARSET_UCS_2 : CharsetUtil.CHARSET_GSM);

                ByteBuffer buffer = ByteBuffer.allocate(udh.length + textBytes.length);
                buffer.put(udh).put(textBytes);

                SubmitSm submitSm = createSubmitSm(isUnicode, true, buffer.array());
                submitAndWait(session, submitSm, String.format("Part %d/%d (Ref %d, %s)", partNum, totalParts, refNum, isUnicode ? "Unicode" : "Plain"));
            }
        }

        private void sendAutoMultipart(SmppSession session, int refNum, String fullMessage, boolean isUnicode) throws Exception {
            byte[] fullBytes = CharsetUtil.encode(fullMessage, isUnicode ? CharsetUtil.CHARSET_UCS_2 : CharsetUtil.CHARSET_GSM);

            int singleMessageLimit = isUnicode ? 140 : 160;
            int multipartChunkLimit = isUnicode ? 134 : 153;

            int totalParts = (fullBytes.length <= singleMessageLimit) ? 1
                    : (int) Math.ceil((double) fullBytes.length / multipartChunkLimit);

            if (totalParts == 1) {
                SubmitSm submitSm = createSubmitSm(isUnicode, false, fullBytes);
                submitAndWait(session, submitSm, String.format("Auto Single Part (Ref %d, %s)", refNum, isUnicode ? "Unicode" : "Plain"));
                return;
            }

            for (int partNum = 1; partNum <= totalParts; partNum++) {
                byte[] udh = buildUdh(refNum, totalParts, partNum);

                int startIdx = (partNum - 1) * multipartChunkLimit;
                int chunkLen = Math.min(multipartChunkLimit, fullBytes.length - startIdx);
                byte[] textBytes = new byte[chunkLen];
                System.arraycopy(fullBytes, startIdx, textBytes, 0, chunkLen);

                ByteBuffer buffer = ByteBuffer.allocate(udh.length + textBytes.length);
                buffer.put(udh).put(textBytes);

                SubmitSm submitSm = createSubmitSm(isUnicode, true, buffer.array());
                submitAndWait(session, submitSm, String.format("Auto Part %d/%d (Ref %d, %s)", partNum, totalParts, refNum, isUnicode ? "Unicode" : "Plain"));
            }
        }

        // --- Helper Methods ---

        private byte[] buildUdh(int refNum, int totalParts, int partNum) {
            return new byte[] { 0x05, 0x00, 0x03, (byte)refNum, (byte)totalParts, (byte)partNum };
        }

        private SubmitSm createSubmitSm(boolean isUnicode, boolean isMultipart, byte[] payload) throws Exception {
            SubmitSm submitSm = new SubmitSm();
            submitSm.setSourceAddress(new Address((byte)0x01, (byte)0x01, "CANBNK"));
            submitSm.setDestAddress(new Address((byte)0x01, (byte)0x01, "917550232158"));
            submitSm.setRegisteredDelivery(SmppConstants.REGISTERED_DELIVERY_SMSC_RECEIPT_REQUESTED);
            submitSm.setDataCoding(isUnicode ? (byte) 0x08 : (byte) 0x00);

            if (isMultipart) {
                submitSm.setEsmClass(SmppConstants.ESM_CLASS_UDHI_MASK);
            }
            submitSm.setShortMessage(payload);
            // Add DLT specific Custom TLVs required by SMSC Core V2 validation
            submitSm.addOptionalParameter(new Tlv((short) 0x1400, "110100001403".getBytes())); // PE_ID
            submitSm.addOptionalParameter(new Tlv((short) 0x1401, "1107174074670190034".getBytes())); // TEMPLATE_ID
            return submitSm;
        }

        private void submitAndWait(SmppSession session, SubmitSm submitSm, String logContext) throws Exception {
            SubmitSmResp submitResp = session.submit(submitSm, 10000);
            log(String.format("Sent %s | Result ID: %s", logContext, submitResp.getMessageId()));
            Thread.sleep(100); // Tiny pause between submissions
        }

        private void unbindSession(SmppSession session) {
            try {
                session.unbind(5000);
                log("Unbound successfully.");
            } catch (Exception e) {
                logError("Failed to unbind cleanly: " + e.getMessage());
            }
        }

        private void log(String message) {
            System.out.println("[Session " + sessionId + "] " + message);
        }

        private void logError(String message) {
            System.err.println("[Session " + sessionId + "] ERROR - " + message);
        }
    }

    /**
     * Handlers for asynchronous events received from the SMSC (like DeliverSm for DLRs or Expired windows).
     */
    private static class ClientSessionHandler extends DefaultSmppSessionHandler {
        @Override
        public void firePduRequestExpired(com.cloudhopper.smpp.pdu.PduRequest pduRequest) {
            System.out.println("PDU request expired: " + pduRequest);
        }

        @Override
        public com.cloudhopper.smpp.pdu.PduResponse firePduRequestReceived(com.cloudhopper.smpp.pdu.PduRequest pduRequest) {
            System.out.println("Received expected PDU Request from server: " + pduRequest);
            return pduRequest.createResponse();
        }
    }
}
