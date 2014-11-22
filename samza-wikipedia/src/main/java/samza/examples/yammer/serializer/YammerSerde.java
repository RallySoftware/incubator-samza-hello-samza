package samza.examples.yammer.serializer;

import org.apache.samza.config.Config;
import org.apache.samza.serializers.JsonSerde;
import org.apache.samza.serializers.Serde;
import org.joda.time.format.ISODateTimeFormat;
import samza.examples.reporter.YammerKey;

import java.util.Map;

import static com.google.common.collect.Maps.newHashMap;
import static samza.examples.reporter.YammerKey.NAME;
import static samza.examples.reporter.YammerKey.TIMESTAMP;
import static samza.examples.reporter.YammerKey.TYPE;

public class YammerSerde implements Serde<YammerKey> {
    private JsonSerde jsonSerde;

    public YammerSerde(final String name, final Config config) {
        jsonSerde = new JsonSerde();
    }

    @Override
    public YammerKey fromBytes(final byte[] bytes) {
        Map<String, String> json = (Map<String, String>) jsonSerde.fromBytes(bytes);
        YammerKey key = new YammerKey();
        key.setName(json.get(NAME));
        key.setType(json.get(TYPE));
        key.setTimestamp(ISODateTimeFormat.dateTime().parseDateTime(json.get(TIMESTAMP)));
        return key;
    }

    @Override
    public byte[] toBytes(final YammerKey key) {
        Map<String, String> json = newHashMap();
        json.put(NAME, key.getName());
        json.put(TYPE, key.getType());
        json.put(TIMESTAMP, ISODateTimeFormat.dateTime().print(key.getTimestamp()));
        return jsonSerde.toBytes(json);
    }
}
