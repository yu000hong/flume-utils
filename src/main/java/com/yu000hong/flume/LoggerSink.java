package com.yu000hong.flume;

import java.nio.charset.StandardCharsets;
import org.apache.flume.Channel;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggerSink extends AbstractSink {

    private static Logger logger = LoggerFactory.getLogger(LoggerSink.class);

    public Status process() throws EventDeliveryException {
        Status result = Status.READY;
        Channel channel = getChannel();
        Transaction transaction = channel.getTransaction();
        Event event = null;

        try {
            transaction.begin();
            event = channel.take();

            if (event != null) {
                if (logger.isInfoEnabled()) {
                    logger.info("Event: " + dump(event));
                }
            } else {
                // No event found, request back-off semantics from the sink runner
                result = Status.BACKOFF;
            }
            transaction.commit();
        } catch (Exception ex) {
            transaction.rollback();
            throw new EventDeliveryException("Failed to log event: " + event, ex);
        } finally {
            transaction.close();
        }

        return result;
    }

    private static String dump(Event event) {
        if (event == null) {
            return "null";
        }
        String body = null;
        if (event.getBody() != null) {
            body = new String(event.getBody(), StandardCharsets.UTF_8);
        }
        return "{ headers:" + event.getHeaders() + " body:" + body + " }";
    }

}
