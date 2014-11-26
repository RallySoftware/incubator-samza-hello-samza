package z.log4j.serializer;

import org.apache.samza.config.Config;
import org.apache.samza.serializers.SerdeFactory;
import samza.examples.appender.Log4JKey;

public class Log4JSerdeFactory implements SerdeFactory<Log4JKey> {
    @Override
    public Log4JSerde getSerde(final String name, final Config config) {
        return new Log4JSerde(name, config);
    }
}
