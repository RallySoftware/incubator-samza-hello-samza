package z.flowdock.system;

import com.google.common.collect.ImmutableList;
import org.apache.samza.Partition;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.SystemStreamMetadata.SystemStreamPartitionMetadata;
import org.apache.samza.system.SystemStreamPartition;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.regex.Pattern;

import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Sets.newHashSet;
import static com.google.common.collect.Sets.newTreeSet;

public class FlowdockSystemAdmin implements SystemAdmin {
    private final String token;

    public FlowdockSystemAdmin(String token) {
        this.token = token;
    }

    @Override
    public Map<SystemStreamPartition, String> getOffsetsAfter(final Map<SystemStreamPartition, String> offsets) {
        Map<SystemStreamPartition, String> offsetsAfter = newHashMap();
        for (SystemStreamPartition systemStreamPartition : offsets.keySet()) {
            offsetsAfter.put(systemStreamPartition, null);
        }
        return offsetsAfter;
    }

    @Override
    public Map<String, SystemStreamMetadata> getSystemStreamMetadata(final Set<String> streamNames) {
        System.err.format("Getting system stream metadata for:");
        for (String streamName : streamNames) {
            System.err.format(" %s", streamName);
        }
        System.err.println();
        System.err.flush();

        try {
            List<String> flows = getFlows();

            System.err.format("Found flows:");
            for (String flow : flows) {
                System.err.format(" %s", flow);
            }
            System.err.println();
            System.err.flush();

            Set<Pattern> patterns = newHashSet();
            for (String name : streamNames) {
                patterns.add(Pattern.compile(name));
            }

            System.err.format("Checking for matching flows\n");
            System.err.flush();

            Map<String, SystemStreamMetadata> metadataMap = newHashMap();
            for (String flow : flows) {
                System.err.format("Checking flow %s\n", flow);
                System.err.flush();

                for (Pattern pattern : patterns) {
                    System.err.format("Checking against pattern %s\n", pattern);
                    System.err.flush();

                    if (pattern.matcher(flow).matches()) {
                        System.err.format("Pattern %s matches flow %s\n", pattern, flow);
                        System.err.flush();

                        Map<Partition, SystemStreamPartitionMetadata> partitionMetadata = newHashMap();
                        partitionMetadata.put(new Partition(0),
                                              new SystemStreamPartitionMetadata(null, null, null));
                        metadataMap.put(flow, new SystemStreamMetadata(flow, partitionMetadata));
                        break;
                    }
                }
            }

            System.err.format("Built metadata map: %s\n", metadataMap);
            System.err.flush();
            return metadataMap;
        } catch (IOException e) {
            e.printStackTrace();
            return newHashMap();
        }
    }

    private List<String> getFlows() throws IOException {
        System.err.format("Fetching all flows\n");
        System.err.flush();

        URL url = new URL("https://api.flowdock.com/flows?users=0");
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("GET");
        connection.setRequestProperty("Accept", "application/json");
        connection.setRequestProperty("Authorization", "Basic " + token);
        connection.setDoInput(true);

        System.err.format("Reading in JSON response\n");
        System.err.flush();

        ObjectMapper objectMapper = new ObjectMapper();
        List json = objectMapper.readValue(connection.getInputStream(), List.class);

        System.err.format("Formatting flows\n");
        System.err.flush();

        SortedSet<String> flows = newTreeSet();
        for (Object flow : json) {
            System.err.format("Formatting flow %s\n", flow);
            System.err.flush();

            Map<String, Object> flowJson = (Map<String, Object>) flow;
            Map<String, Object> organizationJson = (Map<String, Object>) flowJson.get("organization");
            String name = String.format("%s/%s",
                                        organizationJson.get("parameterized_name"),
                                        flowJson.get("parameterized_name"));

            System.err.format("Adding flow %s\n", name);
            System.err.flush();
            flows.add(name);
        }

        System.err.format("Got list of flows: %s\n", flows);
        System.err.flush();

        return ImmutableList.copyOf(flows);
    }
}
