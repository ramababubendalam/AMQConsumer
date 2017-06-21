package com.adstream.automate.test;

import com.adstream.automate.gdn.activemq.ActiveMQService;
import com.adstream.automate.gdn.activemq.Consumer;
import com.adstream.automate.gdn.activemq.Listener;
import com.adstream.automate.gdn.activemq.LoggingListener;
import com.adstream.automate.utils.Common;
import org.apache.commons.lang.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import javax.jms.MessageConsumer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by Ramababu.Bendalam on 19/06/2017.
 */
public class MessageFilter {


    public ActiveMQService activeMQService;
    protected final static Logger log = LoggerFactory.getLogger(MessageFilter.class);
    BlockingQueue<TextMessage> drop;
    protected LoggingListener listener;


    protected LoggingListener getListener() {
        if (null == listener) {
            listener = new Listener(getMessageQ()) {
            };
            listener.setName("Unnamed Engine");
        }
        return listener;
    }

    protected BlockingQueue<TextMessage> getMessageQ() {
        if (drop == null) {
            drop = new LinkedBlockingQueue<TextMessage>();

        }
        return drop;
    }

      public List<String> consume(int expectedCount, long timeout, boolean expectEmptyResult, boolean throwErrorIfNoMessage, String url, Destination destination, String... filters) throws JMSException {
        int totalGot = 0;
        List<TextMessage> filteredMessages = new ArrayList<TextMessage>();
        long exitTime = System.currentTimeMillis() + timeout;
        start(url,destination);
        while (System.currentTimeMillis() < exitTime) {
            log.debug(String.format("Consuming messages[%s] ", ArrayUtils.toString(filters)));
            log.debug(String.format("Current: %s Exit: %s Diff: %s", System.currentTimeMillis(), exitTime, exitTime - System.currentTimeMillis()));
            List<TextMessage> filter = filter(deQMessages(getMessageQ()), filters);
            filteredMessages.addAll(filter);
            totalGot += filteredMessages.size();
            if (filteredMessages.size() > 0 || filters.length == 0) {
                log.debug(String.format("expected count: %s, got: %s, filteredMessages size: %s, filters: %s", expectedCount, totalGot, filteredMessages.size(), ArrayUtils.toString(filters)));

                if (filteredMessages.size() >= expectedCount) {
                      stop(destination);
                    return unpackConsumedMessages(filteredMessages);
                }
            }

            Common.sleep(1000);
        }
        stop(destination);
        if (expectEmptyResult) {

            return unpackConsumedMessages(filteredMessages);
        }

        if (throwErrorIfNoMessage)

            throw new Error("Time Out after " + timeout / 1000 + "sec, message filter. No " + ArrayUtils.toString(filters) + "message recieved");

        return null;
    }

    public List<String> unpackConsumedMessages(List<TextMessage> messages) {
        List<String> unpackedMessages = new ArrayList<String>();
        for (TextMessage textMessage : messages) {
            try {
                unpackedMessages.add(textMessage.getText());
            } catch (JMSException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
        }

        return unpackedMessages;
    }

    protected List<TextMessage> deQMessages(BlockingQueue<TextMessage> q) {
        List<TextMessage> result = new ArrayList<TextMessage>();
        while (q.size() > 0) {
            try {
                result.add(getMessageQ().take());
                } catch (InterruptedException e) {
                log.error(e.toString());
                e.printStackTrace();
            }
        }
        return result;
    }

    protected List<TextMessage> filter(List<TextMessage> messages, String... filters) {
        List<TextMessage> result = new ArrayList<TextMessage>();
        for (TextMessage message : messages) {

            /*
            *  Catch UnknownMessageException for job
            * */

            String[] tempFilters = new String[filters.length + 1];

            tempFilters[0] = "UnknownMessageException";

            for (int i = 0; i < filters.length; i++) {
                tempFilters[i + 1] = filters[i].replace("JobResponse", "Job");
            }

            boolean expectUME = false;
            for (String f : filters) {
                if (f.contains(tempFilters[0])) {
                    expectUME = true;
                }
            }

            if (expectUME) {
                if (isMessageAcceptable(message, tempFilters)) {
                    result.add(message);
                }
            } else {
                if (isMessageAcceptable(message, tempFilters)) {
                    log.error(message.toString());
                    throw new Error("UnknownMessageException message received for sent job");
                }

                if (isMessageAcceptable(message, filters)) {
                    result.add(message);
                }
            }


        }
        log.debug(String.format("Filtering: in[%s]/out[%s]", result.size(), messages.size()));
        return result;
    }

    protected boolean isMessageAcceptable(TextMessage message, String... filters) {
        String[] notNullFilters = removeNullAndEmptyElements(filters);
        int matchesCount = 0;
        for (String filter : notNullFilters) {
            if (getText(message).contains(filter)) {
                matchesCount++;
            }
        }
        return matchesCount == notNullFilters.length;
    }

    public static String[] removeNullAndEmptyElements(String stringArray[]) {
        List<String> list = new ArrayList<String>();

        for (String s : stringArray) {
            if (s != null && s.length() > 0) {
                list.add(s);
            }
        }
        return list.toArray(new String[list.size()]);
    }

    protected String getText(TextMessage message) {
        if (message == null) return "";
        String text = "";
        try {
            text = message.getText();
        } catch (JMSException e) {
            log.error(e.toString());
            e.printStackTrace();
        }
        return text;
    }

    public void stop(Destination destination) throws JMSException {
        activeMQService.createConsumer(destination).endConsuming();
        activeMQService.closeConnection();

    }

    public void start(String url, Destination destination) throws JMSException {
        activeMQService = new ActiveMQService(url);
        activeMQService.createConsumer(destination).startConsuming(getListener());
    }

}
