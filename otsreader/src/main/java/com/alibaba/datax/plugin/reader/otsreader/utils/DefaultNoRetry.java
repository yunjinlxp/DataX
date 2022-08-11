package com.alibaba.datax.plugin.reader.otsreader.utils;


import com.alicloud.openservices.tablestore.model.DefaultRetryStrategy;
import com.alicloud.openservices.tablestore.model.RetryStrategy;

import java.util.concurrent.TimeUnit;

public class DefaultNoRetry extends DefaultRetryStrategy {

    public DefaultNoRetry() {
        super();
    }

    public DefaultNoRetry(long timeout, TimeUnit unit) {
        super(timeout, unit);
    }

    @Override
    public RetryStrategy clone() {
        return super.clone();
    }

    @Override
    public int getRetries() {
        return super.getRetries();
    }

    @Override
    public boolean shouldRetry(String action, Exception ex) {
        return false;
    }

    @Override
    public long nextPause(String action, Exception ex) {
        return super.nextPause(action, ex);
    }
}