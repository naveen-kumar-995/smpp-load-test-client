package com.ptsl.beacon.async;

import com.cloudhopper.smpp.type.Address;

public final class SmsFragment {
    public final byte[] payload;
    public final boolean hasUdh;
    public final Address source;
    public final Address destination;
    public final byte dataCoding;

    public SmsFragment(byte[] payload, boolean hasUdh, Address source, Address destination, byte dataCoding) {
        this.payload = payload;
        this.hasUdh = hasUdh;
        this.source = source;
        this.destination = destination;
        this.dataCoding = dataCoding;
    }
}
