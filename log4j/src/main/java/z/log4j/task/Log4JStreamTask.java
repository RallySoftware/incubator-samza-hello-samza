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

package z.log4j.task;

import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;
import z.appender.Log4JKey;
import z.influx.InfluxKey;
import z.splunk.SplunkKey;

import java.util.Map;

import static com.google.common.collect.Maps.newHashMap;
import static z.appender.Log4JKey.HOST;

public class Log4JStreamTask implements StreamTask {
    public static final String SPLUNK = "splunk";
    public static final String STREAM = "log4j";
    public static final String INFLUX = "influx";
    public static final String MESSAGE = "message";

    @Override
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
        Log4JKey key = (Log4JKey) envelope.getKey();
        String message = (String) envelope.getMessage();

        splunk(key, message, collector, coordinator);
        influx(key, message, collector, coordinator);
    }

    private void splunk(Log4JKey key, String message, MessageCollector collector, TaskCoordinator coordinator) {
        SplunkKey splunkKey = new SplunkKey();
        splunkKey.setSourceType(key.getService());
        splunkKey.setSource(key.getLog());
        splunkKey.setHost(key.getHost());
        splunkKey.setTimestamp(key.getTimestamp());

        SystemStream systemStream = new SystemStream(SPLUNK, STREAM);
        OutgoingMessageEnvelope outgoingMessage = new OutgoingMessageEnvelope(systemStream, splunkKey, message);
        collector.send(outgoingMessage);
    }

    private void influx(Log4JKey key, String message, MessageCollector collector, TaskCoordinator coordinator) {
        InfluxKey influxKey = new InfluxKey();
        influxKey.setSerie(String.format("%s.%s", key.getService(), key.getLog()));
        influxKey.setTimestamp(key.getTimestamp());

        Map<String, Object> influxValue = newHashMap();
        influxValue.put(MESSAGE, message);
        influxValue.put(HOST, key.getHost());

        SystemStream systemStream = new SystemStream(INFLUX, "log4j");
        OutgoingMessageEnvelope outgoingMessage = new OutgoingMessageEnvelope(systemStream, influxKey, influxValue);
        collector.send(outgoingMessage);
    }
}
