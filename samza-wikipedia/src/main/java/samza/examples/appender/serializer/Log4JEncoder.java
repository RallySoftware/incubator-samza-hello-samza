package samza.examples.appender.serializer;

import kafka.serializer.Encoder;
import kafka.serializer.JsonEncoder;
import kafka.utils.VerifiableProperties;
import org.joda.time.format.ISODateTimeFormat;
import samza.examples.appender.Log4JKey;

import java.util.Map;

import static com.google.common.collect.Maps.newHashMap;

public class Log4JEncoder implements Encoder<Log4JKey> {

    private JsonEncoder jsonEncoder;

    public Log4JEncoder(VerifiableProperties properties) {
        jsonEncoder = new JsonEncoder(properties);
    }

    @Override
    public byte[] toBytes(final Log4JKey key) {
        Map<String, Object> json = newHashMap();
        json.put(Log4JKey.SERVICE, key.getService());
        json.put(Log4JKey.LOG, key.getLog());
        json.put(Log4JKey.HOST, key.getHost());
        json.put(Log4JKey.TIMESTAMP, ISODateTimeFormat.dateTime().print(key.getTimestamp()));
        return jsonEncoder.toBytes(json);
    }
}
