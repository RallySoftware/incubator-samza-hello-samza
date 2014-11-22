package samza.examples.reporter.serializer;

import kafka.serializer.Encoder;
import kafka.serializer.JsonEncoder;
import kafka.utils.VerifiableProperties;
import org.joda.time.format.ISODateTimeFormat;
import samza.examples.reporter.YammerKey;

import java.util.Map;

import static com.google.common.collect.Maps.newHashMap;
import static samza.examples.reporter.YammerKey.NAME;
import static samza.examples.reporter.YammerKey.TIMESTAMP;
import static samza.examples.reporter.YammerKey.TYPE;

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
