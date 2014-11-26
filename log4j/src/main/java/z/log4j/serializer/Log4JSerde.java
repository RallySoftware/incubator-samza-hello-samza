package z.log4j.serializer;

import org.apache.samza.config.Config;
import org.apache.samza.serializers.JsonSerde;
import org.apache.samza.serializers.Serde;
import org.joda.time.format.ISODateTimeFormat;
import z.appender.Log4JKey;

import java.util.Map;

import static com.google.common.collect.Maps.newHashMap;
import static z.appender.Log4JKey.*;

public class Log4JSerde implements Serde<Log4JKey> {

    private JsonSerde jsonSerde;

    public Log4JSerde(final String name, final Config config) {
        jsonSerde = new JsonSerde();
    }

    @Override
    public Log4JKey fromBytes(final byte[] bytes) {
        Map<String, String> json = (Map<String, String>) jsonSerde.fromBytes(bytes);
        Log4JKey key = new Log4JKey();
        key.setService(json.get(SERVICE));
        key.setLog(json.get(LOG));
        key.setHost(json.get(HOST));
        key.setTimestamp(ISODateTimeFormat.dateTime().parseDateTime(json.get(TIMESTAMP)));
        return key;
    }

    @Override
    public byte[] toBytes(final Log4JKey key) {
        Map<String, String> json = newHashMap();
        json.put(SERVICE, key.getService());
        json.put(LOG, key.getLog());
        json.put(HOST, key.getHost());
        json.put(TIMESTAMP, ISODateTimeFormat.dateTime().print(key.getTimestamp()));
        return jsonSerde.toBytes(json);
    }
}
