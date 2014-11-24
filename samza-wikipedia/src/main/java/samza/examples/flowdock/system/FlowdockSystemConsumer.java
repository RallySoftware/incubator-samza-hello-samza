package samza.examples.flowdock.system;

import org.apache.samza.Partition;
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

public class FlowdockSystemConsumer implements SystemConsumer {
    private String organization;
    private String token;
    private String[] flows;

    private InputStream stream;
    private JsonSerde serde;

    public FlowdockSystemConsumer(final String organization, final String token, String[] flows) {
        this.organization = organization;
        this.token = token;
        this.flows = flows;
        serde = new JsonSerde();
    }

    @Override
    public void start() {
        getStream();
    }

    private void resetConnection() {
        stream = null;
    }

    private InputStream getStream() {
        if (stream == null) {
            try {
                StringBuilder builder = new StringBuilder();
                builder.append("https://stream.flowdock.com:443/flows?filter=");

                for (String flow : flows) {
                    builder.append(organization);
                    builder.append("/");
                    builder.append(flow);
                    builder.append(",");
                }

                URL url = new URL(builder.toString());

                HttpURLConnection connection = (HttpURLConnection) url.openConnection();
                connection.setRequestMethod("GET");
                connection.setRequestProperty("Accept", "application/json");
                connection.setRequestProperty("Authorization", "Basic " + token);
                connection.setDoInput(true);
                stream = connection.getInputStream();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return stream;
    }

    @Override
    public void stop() {
        try {
            if (stream != null) {
                stream.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void register(final SystemStreamPartition systemStreamPartition, final String offset) {
    }

    @Override
    public List<IncomingMessageEnvelope> poll(final Map<SystemStreamPartition, Integer> systemStreamPartitions, final long timeout) throws InterruptedException {
        List<IncomingMessageEnvelope> messages = newArrayList();

        try {
            InputStream stream = getStream();

            if (stream.available() > 0) {
                StringBuffer buffer = new StringBuffer();
                int b = stream.read();
                while ((char) b != '\r') {
                    buffer.append((char) b);
                    b = stream.read();
                }

                String json = buffer.toString();
                SystemStreamPartition systemStreamPartition = new SystemStreamPartition("flowdock",
                                                                                        "messages",
                                                                                        new Partition(0));
                IncomingMessageEnvelope message = new IncomingMessageEnvelope(systemStreamPartition,
                                                                              null,
                                                                              null,
                                                                              serde.fromBytes(json.getBytes()));
                messages.add(message);

            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return messages;
    }
}
