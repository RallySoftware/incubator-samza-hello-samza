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

package samza.examples.yammer.task;

import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;

import java.util.Map;

public class YammerStreamTask implements StreamTask {
    public static final String NAME = "name";
    public static final String TYPE = "type";
    public static final String GAUGE = "gauge";
    public static final String COUNTER = "counter";
    public static final String HISTOGRAM = "histogram";
    public static final String METER = "meter";
    public static final String TIMER = "timer";
    public static final String VALUE = "value";
    public static final String COUNT = "count";
    public static final String MAX = "max";
    public static final String MEAN = "mean";
    public static final String MIN = "min";
    public static final String STDDEV = "stddev";
    public static final String P_50 = "p50";
    public static final String P_75 = "p75";
    public static final String P_95 = "p95";
    public static final String P_98 = "p98";
    public static final String P_99 = "p99";
    public static final String P_999 = "p999";
    public static final String ONE_MINUTE_RATE = "one-minute-rate";
    public static final String FIVE_MINUTE_RATE = "five-minute-rate";
    public static final String FIFTEEN_MINUTE_RATE = "fifteen-minute-rate";
    public static final String MEAN_RATE = "mean-rate";
    public static final String TOPIC = "yammer";
    public static final String TIMESTAMP = "timestamp";
    public static final String TIME = "time";

    @Override
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
        System.err.format("Yammer StreamTask handing message %s\n", envelope);
        System.err.flush();

        Map<String, String> key = (Map<String, String>) envelope.getKey();
        SystemStream systemStream = new SystemStream("yammer", key.get(TYPE));
        OutgoingMessageEnvelope outgoingMessageEnvelope = new OutgoingMessageEnvelope(systemStream,
                                                                                      envelope.getKey(),
                                                                                      envelope.getMessage());
        collector.send(outgoingMessageEnvelope);
    }
}
