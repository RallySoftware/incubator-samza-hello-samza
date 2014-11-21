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

package samza.examples.dump.task;

import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;

public class DumpStreamTask implements StreamTask {
    public DumpStreamTask() {
        System.err.format("Creating Dump StreamTask\n");
        System.err.flush();
    }

    @Override
    public void process(final IncomingMessageEnvelope envelope, final MessageCollector collector, final TaskCoordinator coordinator) throws Exception {
        System.err.format("Dump StreamTask:\n");
        System.err.format("- Collector: %s\n", collector);
        System.err.format("- Task Coordinator: %s\n", coordinator);
        System.err.format("- Incoming message:\n");
        System.err.format("  - System: %s\n", envelope.getSystemStreamPartition().getSystem());
        System.err.format("  - Stream: %s\n", envelope.getSystemStreamPartition().getStream());
        System.err.format("  - Partition: %s\n", envelope.getSystemStreamPartition().getPartition());
        System.err.format("  - Offset: %s\n", envelope.getOffset());
        System.err.format("  - Key: %s\n", envelope.getKey());
        System.err.format("  - Value: %s\n", envelope.getMessage());
        System.err.format("\n");
        System.err.flush();
    }
}
