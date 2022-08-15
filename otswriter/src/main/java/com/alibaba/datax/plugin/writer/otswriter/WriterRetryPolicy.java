package com.alibaba.datax.plugin.writer.otswriter;

import com.alibaba.datax.plugin.writer.otswriter.model.OTSConf;
import com.alicloud.openservices.tablestore.model.RetryStrategy;

public class WriterRetryPolicy implements RetryStrategy {
    private OTSConf conf;
    private int retryTimes;

    public WriterRetryPolicy(OTSConf conf) {
        this.conf = conf;
        retryTimes = 0;
    }

    public boolean shouldRetry(String action, Exception ex, int retries) {
        return retries <= conf.getRetry();
    }

    public long getPauseDelay(String action, Exception ex, int retries) {
        if (retries <= 0) {
            return 0;
        }
        int sleepTime = conf.getSleepInMillisecond() * retries;
        return Math.min(sleepTime, 30000);
    }

    @Override
    public RetryStrategy clone() {
        return new WriterRetryPolicy(conf);
    }

    @Override
    public int getRetries() {
        return retryTimes;
    }

    @Override
    public long nextPause(String s, Exception e) {
        if (retryTimes > conf.getRetry()) {
            return 0;
        }
        int sleepTime = conf.getSleepInMillisecond() * retryTimes;
        return Math.min(sleepTime, 30000);
    }
}
