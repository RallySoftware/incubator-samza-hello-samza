/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package samza.examples.appender;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;
import org.joda.time.DateTime;

import java.util.Properties;

public class SamzaAppender extends AppenderSkeleton {
    public static final String TOPIC = "log4j";

    private String brokerList;
    private String service;
    private String host;

    private Producer<Log4JKey, String> producer;
    private ProducerConfig producerConfig;

    public String getService() {
        return service;
    }

    public void setService(final String service) {
        this.service = service;
    }

    public String getBrokerList() {
        return brokerList;
    }

    public void setBrokerList(String brokerList) {
        this.brokerList = brokerList;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    @Override
    public boolean requiresLayout() {
        return false;
    }

    @Override
    public void activateOptions() {
        try {
            Properties producerProperties = new Properties();
            producerProperties.put("metadata.broker.list", brokerList);
            producerProperties.put("key.serializer.class", "samza.examples.appender.serializer.Log4JEncoder");
            producerProperties.put("serializer.class", "kafka.serializer.StringEncoder");
            producerProperties.put("request.required.acks", "0");

            producerConfig = new ProducerConfig(producerProperties);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Override
    protected synchronized void append(LoggingEvent event) {
        Log4JKey key = new Log4JKey();
        key.setService(service);
        key.setLog(event.getLoggerName());
        key.setHost(host);
        key.setTimestamp(new DateTime(event.getTimeStamp()));

        String value = event.getRenderedMessage();

        KeyedMessage<Log4JKey, String> message = new KeyedMessage<Log4JKey, String>(TOPIC, key, value);
        getProducer().send(message);
    }

    @Override
    public synchronized void close() {
        producer.close();
        producer = null;
    }

    private Producer<Log4JKey, String> getProducer() {
        if (producer == null) {
            producer = new Producer<Log4JKey, String>(producerConfig);
        }

        return producer;
    }
}