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

package samza.examples.wikipedia;

import com.codahale.metrics.*;
import com.codahale.metrics.Timer;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class KafkaReporter extends ScheduledReporter {
    private List<KeyedMessage> messages;
    private Producer producer;

    public KafkaReporter(MetricRegistry registry,
                         String name,
                         MetricFilter filter,
                         TimeUnit rateUnit,
                         TimeUnit durationUnit,
                         String brokerList) {
        super(registry, name, filter, rateUnit, durationUnit);
        Properties producerProperties = new Properties();

        ProducerConfig producerConfig = new ProducerConfig(producerProperties);
        producer = new Producer(producerConfig);
    }

    @Override
    public void report(SortedMap<String, Gauge> gauges,
                       SortedMap<String, Counter> counters,
                       SortedMap<String, Histogram> histograms,
                       SortedMap<String, Meter> meters,
                       SortedMap<String, Timer> timers) {
        messages = new ArrayList<KeyedMessage>();

        for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
            reportGauge(entry.getKey(), entry.getValue());
        }

        for (Map.Entry<String, Counter> entry : counters.entrySet()) {
            reportCounter(entry.getKey(), entry.getValue());
        }

        for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
            reportHistogram(entry.getKey(), entry.getValue());
        }

        for (Map.Entry<String, Meter> entry : meters.entrySet()) {
            reportMeter(entry.getKey(), entry.getValue());
        }

        for (Map.Entry<String, Timer> entry : timers.entrySet()) {
            reportTimer(entry.getKey(), entry.getValue());
        }

        producer.send(messages);
    }

    private void reportGauge(String name, Gauge metric) {

    }


    private void reportCounter(String name, Counter metric) {
        send("yammer-counter", name, metric.getCount());
    }

    private void reportHistogram(String name, Histogram metric) {
        Snapshot snapshot = metric.getSnapshot();

        send("yammer-histogram", name + ".count", metric.getCount());
        send("yammer-histogram", name + ".max", snapshot.getMax());
        send("yammer-histogram", name + ".mean", snapshot.getMean());
        send("yammer-histogram", name + ".min", snapshot.getMin());
        send("yammer-histogram", name + ".stddev", snapshot.getStdDev());
        send("yammer-histogram", name + ".p50", snapshot.getMedian());
        send("yammer-histogram", name + ".p75", snapshot.get75thPercentile());
        send("yammer-histogram", name + ".p95", snapshot.get95thPercentile());
        send("yammer-histogram", name + ".p98", snapshot.get98thPercentile());
        send("yammer-histogram", name + ".p99", snapshot.get99thPercentile());
        send("yammer-histogram", name + ".p999", snapshot.get999thPercentile());
    }

    private void reportMeter(String name, Meter metric) {
        send("yammer-meter", name + ".count", metric.getCount());
        send("yammer-meter", name + ".one-minute-rate", metric.getOneMinuteRate());
        send("yammer-meter", name + ".five-minute-rate", metric.getFiveMinuteRate());
        send("yammer-meter", name + ".fifteen-minute-rate", metric.getFifteenMinuteRate());
        send("yammer-meter", name + ".mean-rate", metric.getMeanRate());
    }

    private void reportTimer(String name, Timer metric) {
        Snapshot snapshot = metric.getSnapshot();

        send("yammer-timer", name + ".max", snapshot.getMax());
        send("yammer-timer", name + ".min", snapshot.getMin());
        send("yammer-timer", name + ".mean", snapshot.getMean());
        send("yammer-timer", name + ".stddev", snapshot.getStdDev());
        send("yammer-timer", name + ".p50", snapshot.getMedian());
        send("yammer-timer", name + ".p75", snapshot.get75thPercentile());
        send("yammer-timer", name + ".p95", snapshot.get95thPercentile());
        send("yammer-timer", name + ".p98", snapshot.get98thPercentile());
        send("yammer-timer", name + ".p99", snapshot.get99thPercentile());
        send("yammer-timer", name + ".p999", snapshot.get999thPercentile());
    }

    private <T> void send(String topic, String name, T value) {
        messages.add(new KeyedMessage(topic, name, value));
    }
}