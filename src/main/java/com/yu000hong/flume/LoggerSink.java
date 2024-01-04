package com.yu000hong.flume;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class LoggerSink extends AbstractSink implements Configurable {

    private final static Logger LOG = LoggerFactory.getLogger(LoggerSink.class);

    private SinkCounter sinkCounter;
    private int batchSize = 100;

    @Override
    public Status process() {
        Channel channel = getChannel();
        Transaction txn = channel.getTransaction();
        txn.begin();
        int total = 0;
        try {
            for (int i = 0; i < batchSize; i++) {
                Event event = channel.take();
                if (event != null) {
                    total++;
                    sink(event);
                }
            }
            if (total == 0) {
                sinkCounter.incrementBatchEmptyCount();
            } else if (total < batchSize) {
                sinkCounter.incrementBatchUnderflowCount();
            } else {
                sinkCounter.incrementBatchCompleteCount();
            }
            txn.commit();
            sinkCounter.addToEventDrainSuccessCount(total);
            return Status.READY;
        } catch (Throwable tx) {
            sinkCounter.incrementEventWriteOrChannelFail(tx);
            try {
                txn.rollback();
            } catch (Exception ex) {
                LOG.error("exception in rollback.", ex);
            }
            LOG.error("transaction rolled back.", tx);
            return Status.BACKOFF;
        } finally {
            txn.close();
        }
    }


    @Override
    public void start() {
        super.start();
        sinkCounter.start();
    }

    @Override
    public void stop() {
        sinkCounter.stop();
        super.stop();
    }

    private static String dump(Event event) {
        if (event == null) {
            return "null";
        }
        String body = null;
        if (event.getBody() != null) {
            body = new String(event.getBody(), StandardCharsets.UTF_8);
        }
        StringBuilder headers = new StringBuilder();
        headers.append("(");
        boolean first = true;
        for (Map.Entry<String, String> kv : event.getHeaders().entrySet()) {
            if (first) {
                first = false;
            } else {
                headers.append(";");
            }
            headers.append(kv.getKey()).append("=").append(kv.getValue());
        }
        headers.append(")");
        return "{ headers:" + headers.toString() + " body:" + body + " }";
    }


    private void sink(Event event) {
        if (LOG.isInfoEnabled()) {
            LOG.info("Event: " + dump(event));
        }
    }

    @Override
    public void configure(Context context) {
        sinkCounter = new SinkCounter(getName());
    }

}
