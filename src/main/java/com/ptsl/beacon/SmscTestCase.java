package com.ptsl.beacon;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import com.cloudhopper.commons.charset.CharsetUtil;
import com.cloudhopper.smpp.*;
import com.cloudhopper.smpp.impl.DefaultSmppClient;
import com.cloudhopper.smpp.impl.DefaultSmppSessionHandler;
import com.cloudhopper.smpp.pdu.*;
import com.cloudhopper.smpp.tlv.Tlv;
import com.cloudhopper.smpp.type.Address;
import com.sun.jdi.event.ThreadDeathEvent;

public class SmscTestCase {

    private static final int    NUMBER_OF_SESSIONS = 7;
    private static final String SMSC_HOST          = "172.25.4.191";
    private static final int    SMSC_PORT          = 2775;
    private static final String SYSTEM_ID          = "naveen_test_01";
    private static final String PASSWORD           = "pass123";

    private static final AtomicInteger REF_COUNTER = new AtomicInteger(1);


    public static void main(String[] args) throws Exception {

        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newCachedThreadPool();
        ScheduledThreadPoolExecutor monitorExecutor = createMonitorExecutor();

        DefaultSmppClient client = new DefaultSmppClient(executor, NUMBER_OF_SESSIONS, monitorExecutor);

        CountDownLatch latch = new CountDownLatch(NUMBER_OF_SESSIONS);

        for (int i = 0; i < NUMBER_OF_SESSIONS; i++) {
            final int sessionId = i;
            new Thread(() -> {
                try {
                    SmppSessionRunner runner = new SmppSessionRunner(client, sessionId);
                    runner.execute();
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            }).start();
        }

        TimeUnit.HOURS.sleep(1);
        latch.await();
        client.destroy();
        executor.shutdownNow();
        monitorExecutor.shutdownNow();
    }

    private static ScheduledThreadPoolExecutor createMonitorExecutor() {
        return (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(1, new ThreadFactory() {
            private final AtomicInteger sequence = new AtomicInteger(0);

            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setName("SmppClientMonitorPool-" + sequence.getAndIncrement());
                return t;
            }
        });
    }

    // ============================================================
    // SESSION RUNNER
    // ============================================================

    private static class SmppSessionRunner {
        private final DefaultSmppClient smppClient;
        private final int               sessionId;

        public SmppSessionRunner(DefaultSmppClient smppClient, int sessionId) {
            this.smppClient = smppClient;
            this.sessionId = sessionId;
        }

        public void execute() throws Exception {

            SmppSessionConfiguration config = createSessionConfiguration();
            SmppSession session = smppClient.bind(config, new ClientSessionHandler());

            startEnquireLinkTask(session);
            System.out.println("Session " + sessionId + " connected");

            runAllTestCases(session);

            Thread.currentThread().join();
        }

        private void startEnquireLinkTask(SmppSession session) {

            ScheduledExecutorService scheduler =
                    Executors.newSingleThreadScheduledExecutor();

            scheduler.scheduleAtFixedRate(() -> {
                try {
                    if (session.isBound()) {
                        EnquireLink el = new EnquireLink();
                        session.enquireLink(el, 10000);
                        System.out.println("EnquireLink sent");
                    }
                } catch (Exception e) {
                    System.out.println("EnquireLink failed: " + e.getMessage());
                }
            }, 10, 30, TimeUnit.SECONDS);
        }




        // ============================================================
        // TEST CASE MATRIX
        // ============================================================

        private void runAllTestCases(SmppSession session) throws Exception {

            List<TestCase> tests = List.of(
                    // plain Single message
//                    new TestCase(false,false,false,false,
//                            "1007161192273631931",
//                            "Thanks for showing interest on CANARA BUDGET LOAN of Canara Bank. Your Regenerated MPIN is {#var#}.Pls enter the same to continue the online application."),

                    // plain Single flash message
//                    new TestCase(false,false,true,false,
//                    "1007161192273631931",
//                    "Thanks for showing interest on CANARA BUDGET LOAN of Canara Bank. Your Regenerated MPIN is {#var#}.Pls enter the same to continue the online application."),

                    // Unicode Single part
//                    new TestCase(true,false,false,false,
//                    "1107162868531893168",
//                    "जन्मदिन की हार्दिक शुभकामनाएं केनरा बैंक Happy Birthday Canara Bank"),

//                     Unicode Single part flash msg
//                    new TestCase(true,false,true,false,
//                    "1107162868531893168",
//                    "जन्मदिन की हार्दिक शुभकामनाएं केनरा बैंक Happy Birthday Canara Bank"),

//
                    // multi  Plain message
                    new TestCase(false, true,false , false,
                            "1007161200664272263",
                            "An amount of {#var#} has been debited to {#var#} on {#var#} towards {#var#} fvg Benf {#var#}, IFSC {#var#}, Benf A/c {#var#}, UTR {#var#}. Total Avail. Bal INR {#var#} -Canara Bank"),

//                    // Unicode Multi part  msg
//                    new TestCase(true, true, true, false,
//                            "1107174074670190034",
//                            "மார்ச் 4 2025 வருகின்ற செவ்வாய் கிழமை அன்று ங்கரன்கோவில் நீதிமன்ற கட்டிட வளாகத்தில் நடைபெறவுள்ள லோக் அதாலத் தற்போது உள்ளூர் விடுமுறை காரணத்தினால் 03.03.2025 திங்கள் கிழமை அன்று மாற்றப்பட்டுள்ளது என்பதை தெரிவித்துக் கொள்கிறோம். கனரா வங்கி மண்டல அலுவலகம் தூத்துக்குடி"),
//

                    new TestCase(false, false, false, false, null, null)

                                          );


            for (TestCase test : tests) {
                if (test.message == null) {
                    continue;
                }
                sendTest(session, test);
                Thread.sleep(500);
            }
        }

        // ============================================================
        // SEND TEST
        // ============================================================

        private void sendTest(SmppSession session, TestCase test) throws Exception {

            byte[] bytes = CharsetUtil.encode(
                    test.message,
                    test.unicode ? CharsetUtil.CHARSET_UCS_2 : CharsetUtil.CHARSET_GSM);

            String templateId = test.templateId;

            if (! test.multipart) {
                SubmitSm sm = createDynamicSubmitSm(
                        test.unicode,
                        false,
                        test.flash,
                        test.tlvPayload,
                        "CANBNK",
                        "917550232158",
                        "110100001403",
                        templateId,
                        bytes);

                submitAndWait(session, sm, test);
                return;
            }

            sendMultipart(session, test, templateId);
        }

        // ============================================================
        // MULTIPART
        // ============================================================

        private void sendMultipart(SmppSession session, TestCase test, String templateId) throws Exception {

            byte[] fullBytes = CharsetUtil.encode(
                    test.message,
                    test.unicode ? CharsetUtil.CHARSET_UCS_2 : CharsetUtil.CHARSET_GSM);

            int chunk = test.unicode ? 134 : 153;
            int totalParts = (int) Math.ceil((double) fullBytes.length / chunk);
            int ref = REF_COUNTER.incrementAndGet() & 0xff;

            for (int part = 1; part <= totalParts; part++) {

                int start = (part - 1) * chunk;
                int len = Math.min(chunk, fullBytes.length - start);

                byte[] body = new byte[len];
                System.arraycopy(fullBytes, start, body, 0, len);

                byte[] udh = buildUdh(ref, totalParts, part);

                ByteBuffer buf = ByteBuffer.allocate(udh.length + body.length);
                buf.put(udh);
                buf.put(body);

                SubmitSm sm = createDynamicSubmitSm(
                        test.unicode,
                        true,
                        test.flash,
                        test.tlvPayload,
                        "CANBNK",
                        "917550232158",
                        "110100001403",
                        templateId,
                        buf.array());

                submitAndWait(session, sm, test);
            }
        }

        // ============================================================
        // SUBMIT BUILDER
        // ============================================================

        private SubmitSm createDynamicSubmitSm(boolean isUnicode, boolean isMultipart, boolean isFlash,
                                               boolean sendPayloadInTLV, String header, String dest,
                                               String peId, String templateId, byte[] payload) throws Exception {

            SubmitSm submitSm = new SubmitSm();
            submitSm.setSourceAddress(new Address((byte) 1, (byte) 1, header));
            submitSm.setDestAddress(new Address((byte) 1, (byte) 1, dest));
            submitSm.setRegisteredDelivery(SmppConstants.REGISTERED_DELIVERY_SMSC_RECEIPT_REQUESTED);

            byte dcs = isUnicode ? (byte) 0x08 : (byte) 0x00;
            if (isFlash) {
                dcs = isUnicode ? (byte) 0x18 : (byte) 0x10;
            }
            submitSm.setDataCoding(dcs);

            if (isMultipart)
                submitSm.setEsmClass(SmppConstants.ESM_CLASS_UDHI_MASK);

            if (sendPayloadInTLV)
                submitSm.addOptionalParameter(new Tlv(SmppConstants.TAG_MESSAGE_PAYLOAD, payload));
            else
                submitSm.setShortMessage(payload);

            submitSm.addOptionalParameter(new Tlv((short) 0x1400, peId.getBytes()));
            submitSm.addOptionalParameter(new Tlv((short) 0x1401, templateId.getBytes()));

            return submitSm;
        }

        private void submitAndWait(SmppSession session, SubmitSm submitSm, TestCase test) throws Exception {
            SubmitSmResp resp = session.submit(submitSm, 10000);

            System.out.println("Sent -> unicode=" + test.unicode +
                    " multipart=" + test.multipart +
                    " flash=" + test.flash +
                    " tlv=" + test.tlvPayload +
                    " msgId=" + resp.getMessageId());
        }

        private byte[] buildUdh(int refNum, int totalParts, int partNum) {
            return new byte[]{0x05, 0x00, 0x03, (byte) refNum, (byte) totalParts, (byte) partNum};
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
            config.getLoggingOptions().setLogPdu(true);
            return config;
        }
    }

    // ============================================================
    // TEST CASE MODEL
    // ============================================================

    static class TestCase {
        boolean unicode;
        boolean multipart;
        boolean flash;
        boolean tlvPayload;
        String  templateId;
        String  message;

        TestCase(boolean unicode, boolean multipart, boolean flash, boolean tlvPayload, String templateId, String message) {
            this.unicode = unicode;
            this.multipart = multipart;
            this.flash = flash;
            this.tlvPayload = tlvPayload;
            this.templateId = templateId;
            this.message = message;
        }
    }

    private void sendEnquireLink(SmppSession session) throws Exception {


        EnquireLink ping = new EnquireLink();
        session.enquireLink(ping, 10000);
        System.out.println("EnquireLink (Ping) successful!");
    }


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