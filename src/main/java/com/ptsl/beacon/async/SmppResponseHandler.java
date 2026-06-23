package com.ptsl.beacon.async;

import com.cloudhopper.smpp.PduAsyncResponse;
import com.cloudhopper.smpp.impl.DefaultSmppSessionHandler;
import com.cloudhopper.smpp.pdu.DeliverSm;
import com.cloudhopper.smpp.pdu.PduRequest;
import com.cloudhopper.smpp.pdu.PduResponse;
import com.cloudhopper.smpp.pdu.SubmitSmResp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SmppResponseHandler extends DefaultSmppSessionHandler {
    private static final Logger log = LoggerFactory.getLogger(SmppResponseHandler.class);
    private final MetricsTracker metricsTracker;
    private final String sessionName;

    public SmppResponseHandler(String sessionName, MetricsTracker metricsTracker) {
        this.sessionName = sessionName;
        this.metricsTracker = metricsTracker;
    }

    @Override
    public void fireChannelUnexpectedlyClosed() {
        log.error("SMPP session {} closed unexpectedly", sessionName);
    }

    @Override
    public void fireExpectedPduResponseReceived(PduAsyncResponse pduAsyncResponse) {
        PduResponse response = pduAsyncResponse.getResponse();
        if (response instanceof SubmitSmResp) {
            metricsTracker.sent.incrementAndGet();
            if (response.getCommandStatus() == 0) {
                metricsTracker.success.incrementAndGet();
            } else {
                metricsTracker.failed.incrementAndGet();
                log.debug("SubmitSm failed on session {} with status={}", sessionName, response.getCommandStatus());
            }
        }
    }

    @Override
    public void fireUnexpectedPduResponseReceived(PduResponse pduResponse) {
        log.warn("Unexpected PDU response received on session {}: {}", sessionName, pduResponse);
    }

    @Override
    public void firePduRequestExpired(PduRequest pduRequest) {
        metricsTracker.failed.incrementAndGet();
        log.warn("PDU request expired (timed out) on session {}: {}", sessionName, pduRequest);
    }

    @Override
    public PduResponse firePduRequestReceived(PduRequest request) {
        if (request instanceof DeliverSm) {
            metricsTracker.dlrReceived.incrementAndGet();
            DeliverSm deliverSm = (DeliverSm) request;
            log.debug("DLR received on {}: {}", sessionName, new String(deliverSm.getShortMessage()));
        }
        return request.createResponse();
    }
}
