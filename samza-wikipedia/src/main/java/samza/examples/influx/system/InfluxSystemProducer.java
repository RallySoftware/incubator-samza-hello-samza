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

package samza.examples.influx.system;

import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemProducer;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Serie;
import samza.examples.influx.InfluxKey;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Sets.newHashSet;

public class InfluxSystemProducer implements SystemProducer {
    public static final String TIME = "time";

    private String database;
    private InfluxDB connection;

    private Map<String, List<OutgoingMessageEnvelope>> series;
    private int bufferSize;

    public InfluxSystemProducer(String uri, String username, String password, String database, int bufferSize) {
        connection = InfluxDBFactory.connect(uri, username, password);
        this.database = database;
        this.bufferSize = bufferSize;
        series = newHashMap();
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
        InfluxKey key = (InfluxKey) envelope.getKey();
        List<OutgoingMessageEnvelope> serie = series.get(key.getSerie());

        if (serie == null) {
            serie = new ArrayList<OutgoingMessageEnvelope>();
            series.put(key.getSerie(), serie);

        }

        serie.add(envelope);

        if (serie.size() >= bufferSize) {
            flush(source);
        }
    }

    @Override
    public void flush(String source) {
        for (Map.Entry<String, List<OutgoingMessageEnvelope>> entry : series.entrySet()) {
            String serieName = entry.getKey();
            Set<String> columnSet = newHashSet();
            List<OutgoingMessageEnvelope> serie = entry.getValue();

            for (OutgoingMessageEnvelope envelope : serie) {
                Map<String, Object> value = (Map<String, Object>) envelope.getMessage();
                columnSet.addAll(value.keySet());
            }

            Serie.Builder builder = new Serie.Builder(serieName);
            List<String> columnNames = newArrayList(columnSet);
            builder.columns(columnNames.toArray(new String[columnNames.size()]));

            for (OutgoingMessageEnvelope envelope : serie) {
                InfluxKey key = (InfluxKey) envelope.getKey();
                Map<String, Object> value = (Map<String, Object>) envelope.getMessage();

                List<Object> values = newArrayList();
                for (String column : columnSet) {
                    if (TIME.equals(column)) {
                        values.add(key.getTimestamp().getMillis());
                    } else {
                        values.add(value.get(column));
                    }
                }

                builder.values(values.toArray(new Object[values.size()]));
            }

            connection.write(database, TimeUnit.MILLISECONDS, builder.build());
            serie.clear();
        }
    }
}
