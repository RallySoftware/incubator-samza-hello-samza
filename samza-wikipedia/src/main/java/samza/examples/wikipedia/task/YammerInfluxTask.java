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

package samza.examples.wikipedia.task;

import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;
import samza.examples.wikipedia.system.InfluxSystemProducer;

public class YammerInfluxTask implements StreamTask {
    @Override
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
        SystemStreamPartition systemStreamPartition = envelope.getSystemStreamPartition();
        SystemStream systemStream = systemStreamPartition.getSystemStream();

        String inputStream = systemStream.getStream();

        OutgoingMessageEnvelope outgoingMessage;

        if (inputStream.equals("yammer-counter")) {
        } else if (inputStream.equals("yammer-histogram")) {
        } else if (inputStream.equals("yammer-meter")) {
        } else if (inputStream.equals("yammer-timer")) {
        }

//        collector.send(outgoingMessage);
    }
}
