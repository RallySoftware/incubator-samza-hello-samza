package z.yammer.serializer;

import org.apache.samza.config.Config;
import org.apache.samza.serializers.SerdeFactory;
import samza.examples.reporter.YammerKey;

public class YammerSerdeFactory implements SerdeFactory<YammerKey> {
    @Override
    public YammerSerde getSerde(final String name, final Config config) {
        return new YammerSerde(name, config);
    }
}
