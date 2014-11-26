package z.flowdock.system;

import org.apache.samza.serializers.JsonSerde;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemStreamPartition;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;

public class FlowdockSystemConsumer implements SystemConsumer {
    private Map<SystemStreamPartition, InputStream> flows;
    private String token;

    private JsonSerde serde;

    public FlowdockSystemConsumer(final String token) {
        this.flows = newHashMap();
        this.token = token;
        serde = new JsonSerde();
    }

    @Override
    public void start() {
    }

    @Override
    public void stop() {
        try {
            for (InputStream stream : flows.values()) {
                if (stream != null) {
                    stream.close();
                }
            }

            flows.clear();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void register(final SystemStreamPartition systemStreamPartition, final String offset) {
        try {
            getStream(systemStreamPartition);
        } catch (IOException e) {
            System.err.format("Error registering system %s, stream %s, partition %d\n",
                              systemStreamPartition.getSystem(),
                              systemStreamPartition.getStream(),
                              systemStreamPartition.getPartition().getPartitionId());
            System.err.flush();

            e.printStackTrace();
        }
    }

    @Override
    public List<IncomingMessageEnvelope> poll(final Map<SystemStreamPartition, Integer> systemStreamPartitions, final long timeout) throws InterruptedException {
        List<IncomingMessageEnvelope> messages = newArrayList();

        SystemStreamPartition systemStreamPartition = null;
        try {
            for (Map.Entry<SystemStreamPartition, Integer> entry : systemStreamPartitions.entrySet()) {
                systemStreamPartition = entry.getKey();

                InputStream stream = getStream(systemStreamPartition);
                if (stream.markSupported()) {
                    stream.mark(9000);
                }

                if (stream.available() > 1) {
                    StringBuilder buffer = new StringBuilder();
                    int b = stream.read();
                    while ((char) b != '\r') {
                        buffer.append((char) b);
                        b = stream.read();

                        if (stream.available() == 0) {
                            if (stream.markSupported()) {
                                stream.reset();
                            }

                            break;
                        }
                    }

                    String json = buffer.toString();
                    IncomingMessageEnvelope message = new IncomingMessageEnvelope(systemStreamPartition,
                                                                                  null,
                                                                                  null,
                                                                                  serde.fromBytes(json.getBytes()));

                    System.err.format("Adding message to system %s, stream %s, partition %d: %s\n",
                                      systemStreamPartition.getSystem(),
                                      systemStreamPartition.getStream(),
                                      systemStreamPartition.getPartition().getPartitionId(),
                                      json);
                    messages.add(message);

                }
            }
        } catch (IOException e) {
            e.printStackTrace();

            System.err.format("Clearing connection to system %s, stream %s, partition %d\n",
                              systemStreamPartition.getSystem(),
                              systemStreamPartition.getStream(),
                              systemStreamPartition.getPartition().getPartitionId());
            System.err.flush();

            InputStream stream = flows.get(systemStreamPartition);
            if (stream != null) {
                try {
                    stream.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                } finally {
                    flows.remove(systemStreamPartition);
                }
            }
        }

        return messages;
    }

    private InputStream getStream(SystemStreamPartition systemStreamPartition) throws IOException {
        InputStream stream = flows.get(systemStreamPartition);

        if (stream == null) {
            URL url = new URL(String.format("https://stream.flowdock.com:443/flows?filter=%s",
                                            systemStreamPartition.getStream()));
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            connection.setRequestProperty("Accept", "application/json");
            connection.setRequestProperty("Authorization", "Basic " + token);
            connection.setDoInput(true);
            stream = connection.getInputStream();
            flows.put(systemStreamPartition, stream);
        }

        return stream;
    }
}