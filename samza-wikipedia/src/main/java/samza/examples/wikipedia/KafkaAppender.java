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

package samza.examples.wikipedia;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;

import java.util.Properties;

public class KafkaAppender extends AppenderSkeleton {
    private String brokerList;
    private String sourceType;
    private String source;
    private String host;

    private Producer<String, String> producer;

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
            producerProperties.put("key.serializer.class", "kafka.serializer.StringEncoder");
            producerProperties.put("request.required.acks", "0");

            ProducerConfig producerConfig = new ProducerConfig(producerProperties);
            producer = new Producer<String, String>(producerConfig);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Override
    protected synchronized void append(LoggingEvent event) {
        String key = String.format("SPLUNK-MESSAGE: sourcetype=%s source=%s host=%s", sourceType, source, host);
        String value = layout.format(event);
        KeyedMessage<String, String> message = new KeyedMessage<String, String>("application-logs", key, value);
        producer.send(message);
    }

    @Override
    public synchronized void close() {
        producer.close();
    }
}