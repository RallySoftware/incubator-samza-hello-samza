package z.jarvis.task;

import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Serie;
import z.flowdock.FlowdockKey;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class JarvisStreamTask implements StreamTask {

    public static final String MESSAGE = "message";
    public static final String EVENT = "event";
    public static final String THINK = "jarvis think";
    public static final String PING = "jarvis ping";
    public static final String CONTENT = "content";
    public static final String FLOW = "flow";
    public static final String USERNAME = "Jarvis";

    @Override
    public void process(final IncomingMessageEnvelope envelope, final MessageCollector collector, final TaskCoordinator coordinator) throws Exception {
        SystemStreamPartition systemStreamPartition = envelope.getSystemStreamPartition();
        Map<String, Object> message = (Map<String, Object>) envelope.getMessage();

        kafka(collector, message);
        ping(systemStreamPartition, collector, message);
        think(systemStreamPartition, collector, message);
    }

    private void kafka(final MessageCollector collector, final Map<String, Object> message) {
        collector.send(new OutgoingMessageEnvelope(new SystemStream("kafka", "flowdock"), message));
    }

    private void think(final SystemStreamPartition systemStreamPartition, final MessageCollector collector, final Map<String, Object> message) {
        if (MESSAGE.equals(message.get(EVENT))) {
            if (THINK.equals(message.get(CONTENT))) {
                FlowdockKey key = new FlowdockKey();
                key.setFlow((String) message.get(FLOW));
                key.setUsername(USERNAME);
                key.setType(MESSAGE);

                OutgoingMessageEnvelope outgoingMessageEnvelope = new OutgoingMessageEnvelope(systemStreamPartition.getSystemStream(),
                                                                                              key,
                                                                                              "Here's what I'm thinking about:");
                collector.send(outgoingMessageEnvelope);

                InfluxDB influx = InfluxDBFactory.connect("http://localhost:8086", "samza", "samza");
                queryToFlowdock(collector, key, influx, "log messages", "log4j");
                queryToFlowdock(collector, key, influx, "yammer metrics", "yammer");
            }
        }
    }

    private void ping(final SystemStreamPartition systemStreamPartition, final MessageCollector collector, final Map<String, Object> message) {
        if (MESSAGE.equals(message.get(EVENT))) {
            if (PING.equals(message.get(CONTENT))) {
                FlowdockKey key = new FlowdockKey();
                key.setFlow((String) message.get(FLOW));
                key.setUsername(USERNAME);
                key.setType(MESSAGE);

                OutgoingMessageEnvelope outgoingMessageEnvelope = new OutgoingMessageEnvelope(systemStreamPartition.getSystemStream(),
                                                                                              key,
                                                                                              "samza");
                collector.send(outgoingMessageEnvelope);
            }
        }
    }

    private void queryToFlowdock(final MessageCollector collector, final FlowdockKey key, final InfluxDB influx, final String label, final String database) {
        for (Serie serie : influx.query(database, "select * from /.*/ limit 10", TimeUnit.MILLISECONDS)) {
            StringBuilder builder = new StringBuilder();
            builder.append("Last 10 ");
            builder.append(label);
            builder.append(" from ");
            builder.append(serie.getName());
            builder.append(":\n    #");

            String[] columnNames = serie.getColumns();
            for (String columnName : serie.getColumns()) {
                builder.append(" ");
                builder.append(columnName);
            }
            builder.append("\n");

            for (Map<String, Object> row : serie.getRows()) {
                builder.append("   ");

                for (String columnName : serie.getColumns()) {
                    builder.append(" ");
                    builder.append(row.get(columnName));
                }

                builder.append("\n");
            }

            OutgoingMessageEnvelope outgoingMessageEnvelope = new OutgoingMessageEnvelope(new SystemStream(
                    "think",
                    "messages"), key, builder.toString());
            collector.send(outgoingMessageEnvelope);
        }
    }
}
