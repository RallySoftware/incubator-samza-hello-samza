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

package samza.examples.log4j;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;
import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;

import java.util.Map;
import java.util.Properties;

import static com.google.common.collect.Maps.newHashMap;
import static samza.examples.log4j.task.Log4JStreamTask.*;

public class SamzaAppender extends AppenderSkeleton {
    private String brokerList;
    private String sourceType;
    private String source;
    private String host;

    private Producer<Map<String, String>, String> producer;
    private ProducerConfig producerConfig;

    public String getBrokerList() {
        return brokerList;
    }

    public void setBrokerList(String brokerList) {
        this.brokerList = brokerList;
    }

    public String getSourceType() {
        return sourceType;
    }

    public void setSourceType(String sourceType) {
        this.sourceType = sourceType;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    @Override
    public boolean requiresLayout() {
        return true;
    }

    @Override
    public void activateOptions() {
        try {
            Properties producerProperties = new Properties();
            producerProperties.put("metadata.broker.list", brokerList);
            producerProperties.put("serializer.class", "kafka.serializer.StringEncoder");
            producerProperties.put("key.serializer.class", "kafka.serializer.JsonEncoder");
            producerProperties.put("request.required.acks", "0");

            producerConfig = new ProducerConfig(producerProperties);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Override
    protected synchronized void append(LoggingEvent event) {
        DateTime timestamp = DateTime.now();
        Map<String, String> key = newHashMap();
        key.put(SOURCETYPE, sourceType);
        key.put(SOURCE, source);
        key.put(HOST, host);
        key.put(TIMESTAMP, ISODateTimeFormat.dateTime().print(timestamp));
        String value = layout.format(event);
        KeyedMessage<Map<String, String>, String> message = new KeyedMessage<Map<String, String>, String>(
                TOPIC, key, value);
        getProducer().send(message);
    }

    @Override
    public synchronized void close() {
        producer.close();
    }

    private Producer<Map<String, String>, String> getProducer() {
        if (producer == null) {
            producer = new Producer<Map<String, String>, String>(producerConfig);
        }

        return producer;
    }
}