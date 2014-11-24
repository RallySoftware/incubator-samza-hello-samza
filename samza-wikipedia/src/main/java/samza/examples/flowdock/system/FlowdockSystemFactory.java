package samza.examples.flowdock.system;

import org.apache.samza.config.Config;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.util.SinglePartitionWithoutOffsetsSystemAdmin;

public class FlowdockSystemFactory implements SystemFactory {
    @Override
    public SystemConsumer getConsumer(String systemName, Config config, MetricsRegistry registry) {
        String token = config.get("systems." + systemName + ".token");
        String organization = config.get("systems." + systemName + ".organization");
        String[] flows = config.get("systems." + systemName + ".flows").split(",");

        return new FlowdockSystemConsumer(organization, token, flows);
    }

    @Override
    public SystemProducer getProducer(String systemName, Config config, MetricsRegistry registry) {
        String token = config.get("systems." + systemName + ".token");

        return new FlowdockSystemProducer(token);
    }

    @Override
    public SystemAdmin getAdmin(String systemName, Config config) {
        return new SinglePartitionWithoutOffsetsSystemAdmin();
    }
}
