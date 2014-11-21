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

import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemProducer;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Serie;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;
import static samza.examples.yammer.task.YammerStreamTask.NAME;

public class YammerSystemProducer implements SystemProducer {
    private String uri;
    private String username;
    private String password;
    private String database;

    private InfluxDB influx;

    private List<OutgoingMessageEnvelope> buffer;
    private int bufferSize;

    public YammerSystemProducer(String uri, String username, String password, String database, int bufferSize) {
        this.uri = uri;
        this.username = username;
        this.password = password;
        this.database = database;
        this.bufferSize = bufferSize;

        buffer = newArrayList();
    }

    @Override
    public void start() {
    }

    @Override
    public void stop() {
    }

    @Override
    public void register(String source) {
    }

    @Override
    public void send(String source, OutgoingMessageEnvelope envelope) {
        buffer.add(envelope);

        if (buffer.size() >= bufferSize) {
            flush(source);
        }
    }

    @Override
    public void flush(String source) {
        Map<String, List<String>> columns = newHashMap();
        Map<String, Serie.Builder> serieBuilders = newHashMap();

        for (OutgoingMessageEnvelope envelope : buffer) {
            Map<String, String> key = (Map<String, String>) envelope.getKey();
            Map<String, Object> value = (Map<String, Object>) envelope.getMessage();


            String seriesName = key.get(NAME);
            Serie.Builder builder = serieBuilders.get(seriesName);
            List<String> columnNames = columns.get(seriesName);
            if (builder == null) {
                builder = new Serie.Builder(seriesName);
                columnNames = newArrayList(value.keySet());

                serieBuilders.put(seriesName, builder);
                columns.put(seriesName, columnNames);

                builder.columns(columnNames.toArray(new String[columnNames.size()]));
            }

            List<Object> values = newArrayList();
            for (String column : columnNames) {
                values.add(value.get(column));
            }
            builder.values(values.toArray(new Object[values.size()]));
        }

        List<Serie> series = newArrayList();
        for (Serie.Builder builder : serieBuilders.values()) {
            Serie serie = builder.build();
            series.add(serie);
        }

        getDatabase().write(database, TimeUnit.MILLISECONDS, series.toArray(new Serie[series.size()]));

        buffer.clear();
    }

    private InfluxDB getDatabase() {
        if (influx == null) {
            influx = InfluxDBFactory.connect(uri, username, password);
        }

        return influx;
    }
}