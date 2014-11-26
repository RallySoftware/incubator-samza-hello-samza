package z.reporter.serializer;

import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;
import org.joda.time.format.ISODateTimeFormat;
import z.kafka.serializer.JsonEncoder;
import z.reporter.YammerKey;

import java.util.Map;

import static com.google.common.collect.Maps.newHashMap;
import static z.reporter.YammerKey.NAME;
import static z.reporter.YammerKey.TIMESTAMP;
import static z.reporter.YammerKey.TYPE;

/**
 * A Kafka Encoder which serializes a {@link z.reporter.YammerKey} to JSON.
 */
public class YammerEncoder implements Encoder<YammerKey> {
    private JsonEncoder jsonEncoder;

    public YammerEncoder(VerifiableProperties properties) {
        jsonEncoder = new JsonEncoder(properties);
    }

    @Override
    public byte[] toBytes(final YammerKey key) {
        Map<String, Object> json = newHashMap();
        json.put(NAME, key.getName());
        json.put(TYPE, key.getType());
        json.put(TIMESTAMP, ISODateTimeFormat.dateTime().print(key.getTimestamp()));
        return jsonEncoder.toBytes(json);
    }
}
