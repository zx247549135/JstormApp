package com.hust.ali.jstorm.example;

import java.io.Serializable;

/**
 * Created by zx on 2016/4/20.
 */
public class IntervalCheck implements Serializable {
    private static final long serialVersionUID = 8952971673547362883L;

    long lastCheck = System.currentTimeMillis();

    // default interval is 1 second
    long interval = 1;

    public Double checkAndGet() {
        long now = System.currentTimeMillis();

        synchronized (this) {
            if (now >= interval * 1000 + lastCheck) {
                double pastSecond = ((double) (now - lastCheck)) / 1000;
                lastCheck = now;
                return pastSecond;
            }
        }

        return null;
    }

    public long getInterval() {
        return interval;
    }

    public void setInterval(long interval) {
        this.interval = interval;
    }

    public void adjust(long addTimeMillis) {
        lastCheck += addTimeMillis;
    }

    public void start() {
        lastCheck = System.currentTimeMillis();
    }
}
