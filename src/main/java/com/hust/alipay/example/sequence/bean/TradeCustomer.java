package com.hust.alipay.example.sequence.bean;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import java.io.Serializable;

/**
 * Created by zx on 2016/4/20.
 */

public class TradeCustomer implements Serializable {
    private static final long serialVersionUID = 1294530416638900059L;
    protected final long timestamp;
    protected Pair trade;
    protected Pair customer;
    protected String buffer;

    public TradeCustomer() {
        timestamp = System.currentTimeMillis();
    }

    public TradeCustomer(long timestamp, Pair trade, Pair customer, String str) {
        this.timestamp = timestamp;
        this.trade = trade;
        this.customer = customer;
        this.buffer = str;
    }

    public Pair getTrade() {
        return trade;
    }

    public void setTrade(Pair trade) {
        this.trade = trade;
    }

    public Pair getCustomer() {
        return customer;
    }

    public void setCustomer(Pair customer) {
        this.customer = customer;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getBuffer() {
        return buffer;
    }

    public void setBuffer(String buffer) {
        this.buffer = buffer;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this,
                ToStringStyle.SHORT_PREFIX_STYLE);
    }

}