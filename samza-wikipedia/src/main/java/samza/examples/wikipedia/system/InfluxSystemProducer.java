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

package samza.examples.wikipedia.system;

import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemProducer;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Serie;

import java.util.concurrent.TimeUnit;

public class InfluxSystemProducer implements SystemProducer {
    private InfluxDB influx;
    private String database;

    public InfluxSystemProducer(String uri, String username, String password, String database) {
        influx = InfluxDBFactory.connect(uri, username, password);
        this.database = database;
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
        String name = (String) envelope.getKey();
        Serie series = new Serie.Builder(name).build();
        influx.write(database, TimeUnit.MILLISECONDS, series);
    }

    @Override
    public void flush(String source) {
    }
}