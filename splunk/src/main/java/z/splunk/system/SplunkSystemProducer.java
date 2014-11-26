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

package z.splunk.system;

import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemProducer;
import org.joda.time.format.ISODateTimeFormat;
import z.splunk.SplunkKey;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.Socket;

public class SplunkSystemProducer implements SystemProducer {
    public static final String TIMESTAMP = "timestamp";
    public static final String HOST = "host";
    public static final String SOURCE = "source";
    public static final String SOURCETYPE = "sourcetype";
    private String host;
    private int port;
    private Socket socket;
    private OutputStreamWriter writer;

    public SplunkSystemProducer(String host, int port) {
        this.host = host;
        this.port = port;
    }

    @Override
    public void start() {
        try {
            open();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void stop() {
        try {
            close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void send(String source, OutgoingMessageEnvelope envelope) {
        SplunkKey key = (SplunkKey) envelope.getKey();
        String message = (String) envelope.getMessage();

        StringBuilder builder = new StringBuilder();

        builder.append("SPLUNK-MESSAGE:");
        appendKeyValue(builder, SOURCETYPE, key.getSourceType());
        appendKeyValue(builder, SOURCE, key.getSource());
        appendKeyValue(builder, HOST, key.getHost());
        appendKeyValue(builder, TIMESTAMP, ISODateTimeFormat.dateTime().print(key.getTimestamp()));

        builder.append("\n");
        builder.append(message);
        builder.append("\n---\n");

        try {
            OutputStreamWriter writer = getWriter();
            writer.write(builder.toString());
            writer.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void appendKeyValue(StringBuilder builder, String key, String value) {
        builder.append(" ");
        builder.append(key);
        builder.append("=\"");
        builder.append(value);
        builder.append("\"");
    }

    @Override
    public void register(String source) {
    }

    @Override
    public void flush(String source) {
        try {
            getWriter().flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private OutputStreamWriter getWriter() throws IOException {
        if (socket == null || writer == null || socket.isClosed()) {
            open();
        }

        return writer;
    }

    private void open() throws IOException {
        close();
        socket = new Socket(host, port);
        writer = new OutputStreamWriter(socket.getOutputStream());
    }

    private void close() throws IOException {
        if (socket != null) {
            writer.flush();
            socket.close();
        }
    }
}