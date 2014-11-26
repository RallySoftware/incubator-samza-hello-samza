package z.flowdock.system;

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
        return new FlowdockSystemConsumer(getToken(systemName, config));
    }

    @Override
    public SystemProducer getProducer(String systemName, Config config, MetricsRegistry registry) {
        return new FlowdockSystemProducer(getToken(systemName, config));
    }

    @Override
    public SystemAdmin getAdmin(String systemName, Config config) {
        return new FlowdockSystemAdmin(getToken(systemName, config));
    }

    private String getToken(final String systemName, final Config config) {
        return config.get("systems." + systemName + ".token");
    }
}
