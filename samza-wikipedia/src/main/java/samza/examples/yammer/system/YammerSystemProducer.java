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

package samza.examples.yammer.system;

import com.google.common.collect.ImmutableList;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemProducer;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Serie;
import org.influxdb.dto.Serie.Builder;
import org.joda.time.format.ISODateTimeFormat;
import samza.examples.yammer.task.YammerStreamTask;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;
import static samza.examples.yammer.task.YammerStreamTask.NAME;
import static samza.examples.yammer.task.YammerStreamTask.TIME;
import static samza.examples.yammer.task.YammerStreamTask.TIMESTAMP;

public class YammerSystemProducer implements SystemProducer {
    private String uri;
    private String username;
    private String password;
    private String database;

    private InfluxDB influx;

    private List<OutgoingMessageEnvelope> buffer;
    private int bufferSize;

    public YammerSystemProducer(String uri, String username, String password, String database, int bufferSize) {
        System.err.format("Creating Yammer SystemProducer\n");
        System.err.flush();
        this.uri = uri;
        this.username = username;
        this.password = password;
        this.database = database;
        this.bufferSize = bufferSize;

        buffer = newArrayList();
    }

    @Override
    public void start() {
        System.err.format("Starting Yammer SystemProducer %s\n", this);
        System.err.flush();
    }

    @Override
    public void stop() {
        System.err.format("Stopping Yammer SystemProducer %s\n", this);
        System.err.flush();
    }

    @Override
    public void register(String source) {
        System.err.format("Registering source %s with Yammer SystemProducer %s\n", source, this);
        System.err.flush();
    }

    @Override
    public void send(String source, OutgoingMessageEnvelope envelope) {
        System.err.format("Sending message from %s via Yammer SystemProducer %s\n", source, this);
        System.err.flush();
        buffer.add(envelope);

        if (buffer.size() > bufferSize) {
            flush(source);
        }
    }

    @Override
    public void flush(String source) {
        System.err.format("Flushing source %s buffer of Yammer SystemProducer %s\n", source, this);
        System.err.flush();

        Map<String, List<String>> columns = newHashMap();
        Map<String, Serie.Builder> serieBuilders = newHashMap();

        for (OutgoingMessageEnvelope envelope : buffer) {
            Map<String, String> key = (Map<String, String>) envelope.getKey();
            Map<String, Object> value = (Map<String, Object>) envelope.getMessage();

            Serie.Builder builder = serieBuilders.get(key.get(NAME));
            List<String> columnNames = columns.get(key.get(NAME));
            if (builder == null) {
                builder = new Serie.Builder(key.get(NAME));
                columnNames = newArrayList(value.keySet());
                columnNames.add(TIME);

                serieBuilders.put(key.get(NAME), builder);
                columns.put(key.get(NAME), columnNames);

                builder.columns(columnNames.toArray(new String[columnNames.size()]));
            }

            List<Object> values = newArrayList();
            for (String column : columnNames) {
                if (TIME.equals(column)) {
                    values.add(ISODateTimeFormat.dateTime().parseDateTime(key.get(TIMESTAMP)).toInstant().getMillis());
                } else {
                    values.add(value.get(column));
                }
            }
            builder.values(values);
        }

        List<Serie> series = newArrayList();
        for (Serie.Builder builder : serieBuilders.values()) {
            series.add(builder.build());
        }

        System.err.format("Writing series from Yammer SystemProducer %s: %s\n", this, series);
        System.err.flush();

        getDatabase().write(database, TimeUnit.MILLISECONDS, series.toArray(new Serie[series.size()]));

        buffer.clear();
    }

    private InfluxDB getDatabase() {
        if (influx == null) {
            System.err.format("Connecting Yammer SystemProducer %s to influx\n", this);
            System.err.flush();

            influx = InfluxDBFactory.connect(uri, username, password);
        }

        return influx;
    }
}