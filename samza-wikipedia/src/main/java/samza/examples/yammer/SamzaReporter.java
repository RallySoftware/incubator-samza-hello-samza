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

package samza.examples.yammer;

import com.codahale.metrics.*;
import com.codahale.metrics.Timer;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.Maps.newHashMap;
import static samza.examples.yammer.task.YammerStreamTask.*;
import static samza.examples.yammer.task.YammerStreamTask.NAME;
import static samza.examples.yammer.task.YammerStreamTask.TYPE;

public class SamzaReporter extends ScheduledReporter {
    private ProducerConfig producerConfig;
    private List<KeyedMessage<Map<String, String>, Map<String, Object>>> messages;
    private Producer<Map<String, String>, Map<String, Object>> producer;

    public static Builder forRegistry(MetricRegistry registry) {
        return new Builder(registry);
    }

    public static class Builder {
        private final MetricRegistry registry;
        private TimeUnit rateUnit;
        private TimeUnit durationUnit;
        private MetricFilter filter;
        private String brokerList;
        private String name;

        private Builder(MetricRegistry registry) {
            this.registry = registry;
            this.rateUnit = TimeUnit.SECONDS;
            this.durationUnit = TimeUnit.MILLISECONDS;
            this.filter = MetricFilter.ALL;
        }

        public Builder convertRatesTo(TimeUnit rateUnit) {
            this.rateUnit = rateUnit;
            return this;
        }

        public Builder convertDurationsTo(TimeUnit durationUnit) {
            this.durationUnit = durationUnit;
            return this;
        }

        public Builder filter(MetricFilter filter) {
            this.filter = filter;
            return this;
        }

        public Builder withBrokerList(String brokerList) {
            this.brokerList = brokerList;
            return this;
        }

        public Builder named(String name) {
            this.name = name;
            return this;
        }

        public SamzaReporter build() {
            return new SamzaReporter(registry, name, filter, rateUnit, durationUnit, brokerList);
        }
    }

    public SamzaReporter(MetricRegistry registry,
                         String name,
                         MetricFilter filter,
                         TimeUnit rateUnit,
                         TimeUnit durationUnit,
                         String brokerList) {
        super(registry, name, filter, rateUnit, durationUnit);

        System.err.format("Creating Yammer Reporter\n");
        System.err.flush();

        Properties producerProperties = new Properties();
        producerProperties.put("metadata.broker.list", brokerList);
        producerProperties.put("serializer.class", "kafka.serializer.JsonEncoder");
        producerProperties.put("key.serializer.class", "kafka.serializer.JsonEncoder");
        producerProperties.put("request.required.acks", "0");
        producerConfig = new ProducerConfig(producerProperties);
    }

    @Override
    public void report(SortedMap<String, Gauge> gauges,
                       SortedMap<String, Counter> counters,
                       SortedMap<String, Histogram> histograms,
                       SortedMap<String, Meter> meters,
                       SortedMap<String, Timer> timers) {
        try {
        System.err.format("Yammer Reporter reporting\n");
        System.err.flush();

        Map<String, String> key;
        Map<String, Object> value;
        DateTime timestamp = DateTime.now();
        messages = new ArrayList<KeyedMessage<Map<String, String>, Map<String, Object>>>();

        for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
            key = newHashMap();
            key.put(NAME, entry.getKey());
            key.put(TYPE, GAUGE);

            value = newHashMap();
            report(entry.getValue(), value);
            value.put(TIMESTAMP, ISODateTimeFormat.dateTime().print(timestamp));

            messages.add(new KeyedMessage<Map<String, String>, Map<String, Object>>(TOPIC, key, value));
        }

        for (Map.Entry<String, Counter> entry : counters.entrySet()) {
            key = newHashMap();
            key.put(NAME, entry.getKey());
            key.put(TYPE, COUNTER);

            value = newHashMap();
            report(entry.getValue(), value);
            value.put(TIMESTAMP, ISODateTimeFormat.dateTime().print(timestamp));

            messages.add(new KeyedMessage<Map<String, String>, Map<String, Object>>(TOPIC, key, value));
        }

        for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
            key = newHashMap();
            key.put(NAME, entry.getKey());
            key.put(TYPE, HISTOGRAM);

            value = newHashMap();
            report(entry.getValue(), value);
            value.put(TIMESTAMP, ISODateTimeFormat.dateTime().print(timestamp));

            messages.add(new KeyedMessage<Map<String, String>, Map<String, Object>>(TOPIC, key, value));
        }

        for (Map.Entry<String, Meter> entry : meters.entrySet()) {
            key = newHashMap();
            key.put(NAME, entry.getKey());
            key.put(TYPE, METER);

            value = newHashMap();
            report(entry.getValue(), value);
            value.put(TIMESTAMP, ISODateTimeFormat.dateTime().print(timestamp));

            messages.add(new KeyedMessage<Map<String, String>, Map<String, Object>>(TOPIC, key, value));
        }

        for (Map.Entry<String, Timer> entry : timers.entrySet()) {
            key = newHashMap();
            key.put(NAME, entry.getKey());
            key.put(TYPE, TIMER);

            value = newHashMap();
            report(entry.getValue(), value);
            value.put(TIMESTAMP, ISODateTimeFormat.dateTime().print(timestamp));

            messages.add(new KeyedMessage<Map<String, String>, Map<String, Object>>(TOPIC, key, value));
        }

        System.err.format("Metrics: %s\n", messages);
        System.err.flush();

        getProducer().send(messages);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void report(Gauge metric, Map<String, Object> json) {
        json.put(VALUE, metric.getValue());
    }


    private void report(Counter metric, Map<String, Object> json) {
        json.put(COUNT, metric.getCount());
    }

    private void report(Histogram metric, Map<String, Object> json) {
        Snapshot snapshot = metric.getSnapshot();
        json.put(COUNT, metric.getCount());
        json.put(MAX, snapshot.getMax());
        json.put(MEAN, snapshot.getMean());
        json.put(MIN, snapshot.getMin());
        json.put(STDDEV, snapshot.getStdDev());
        json.put(P_50, snapshot.getMedian());
        json.put(P_75, snapshot.get75thPercentile());
        json.put(P_95, snapshot.get95thPercentile());
        json.put(P_98, snapshot.get98thPercentile());
        json.put(P_99, snapshot.get99thPercentile());
        json.put(P_999, snapshot.get999thPercentile());
    }

    private void report(Meter metric, Map<String, Object> json) {
        json.put(COUNT, metric.getCount());
        json.put(ONE_MINUTE_RATE, metric.getOneMinuteRate());
        json.put(FIVE_MINUTE_RATE, metric.getFiveMinuteRate());
        json.put(FIFTEEN_MINUTE_RATE, metric.getFifteenMinuteRate());
        json.put(MEAN_RATE, metric.getMeanRate());
    }

    private void report(Timer metric, Map<String, Object> json) {
        Snapshot snapshot = metric.getSnapshot();
        json.put(MAX, snapshot.getMax());
        json.put(MIN, snapshot.getMin());
        json.put(MEAN, snapshot.getMean());
        json.put(STDDEV, snapshot.getStdDev());
        json.put(P_50, snapshot.getMedian());
        json.put(P_75, snapshot.get75thPercentile());
        json.put(P_95, snapshot.get95thPercentile());
        json.put(P_98, snapshot.get98thPercentile());
        json.put(P_99, snapshot.get99thPercentile());
        json.put(P_999, snapshot.get999thPercentile());
    }

    private Producer<Map<String, String>, Map<String, Object>> getProducer() {
        if (producer == null) {
            producer = new Producer<Map<String, String>, Map<String, Object>>(producerConfig);
        }

        return producer;
    }
}