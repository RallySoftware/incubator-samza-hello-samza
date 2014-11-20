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

package samza.examples.log4j.system;

import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemProducer;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.Socket;

public class Log4JSystemProducer implements SystemProducer {
    private String host;
    private int port;
    private Socket socket;
    private OutputStreamWriter writer;

    public Log4JSystemProducer(String host, int port) {
        this.host = host;
        this.port = port;
    }

    @Override
    public void start() {
        try {
            createSocket();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void stop() {
        try {
            if (writer != null) {
                writer.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void send(String source, OutgoingMessageEnvelope envelope) {
        String data = String.format("%s\n%s\n---\n", envelope.getKey(), envelope.getMessage());
        try {
            if (writer != null) {
                writer.write(data);
                writer.flush();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void register(String source) {
    }

    @Override
    public void flush(String source) {
        try {
            if (writer != null) {
                writer.flush();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void createSocket() throws IOException {
        socket = new Socket(host, port);
        writer = new OutputStreamWriter(socket.getOutputStream());
    }
}