package samza.examples.flowdock.system;

import org.apache.samza.serializers.JsonSerde;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemProducer;
import samza.examples.flowdock.FlowdockKey;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;

import static com.google.common.collect.Maps.newHashMap;

public class FlowdockSystemProducer implements SystemProducer {
    private String token;

    public FlowdockSystemProducer(final String token) {
        this.token = token;
    }

    @Override
    public void start() {
    }

    @Override
    public void stop() {

    }

    @Override
    public void register(final String source) {

    }

    @Override
    public void send(final String source, final OutgoingMessageEnvelope envelope) {
        System.err.format("Processing message from %s: %s\n", source, envelope);
        System.err.flush();

        try {
            FlowdockKey key = (FlowdockKey) envelope.getKey();
            String message = (String) envelope.getMessage();

            Map<String, Object> json = newHashMap();
            json.put("flow", key.getFlow());
            json.put("event", key.getType());
            json.put("content", message);

            if (key.getUsername() != null) {
                json.put("external_user_name", key.getUsername());
            }

            if (key.getThread() != null) {
                json.put("thread_id", key.getThread());
            }

            byte[] bytes = new JsonSerde().toBytes(json);
            System.err.format("Sending JSON: %s\n", new String(bytes));
            System.err.flush();

            URL url = new URL(String.format("https://api.flowdock.com/messages"));
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setDoOutput(true);
            connection.setRequestProperty("Content-Type", "application/json");
            connection.setRequestProperty("Authorization", "Basic " + token);

            connection.connect();
            DataOutputStream output = new DataOutputStream(connection.getOutputStream());
            output.write(bytes);
            output.flush();
            output.close();

            System.err.format("Message sent!\n");
            System.err.flush();

            int status = connection.getResponseCode();
            System.err.format("Response status: %d\n", status);
            System.err.flush();

            BufferedReader body = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            System.err.format("Response body:\n");
            System.err.flush();

            for (String line; (line = body.readLine()) != null; ) {
                System.err.format("    %s\n", line);
                System.err.flush();
            }
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void flush(final String source) {

    }
}
