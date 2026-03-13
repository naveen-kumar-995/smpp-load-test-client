package com.ptsl.beacon;

import com.cloudhopper.smpp.pdu.PduRequest;
import com.cloudhopper.smpp.pdu.PduResponse;

public interface Handler {
    void fireExpectedPduResponseReceived(
            PduRequest req,
            PduResponse resp);
}
