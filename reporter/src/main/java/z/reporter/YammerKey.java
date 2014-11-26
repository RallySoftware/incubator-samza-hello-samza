package z.reporter;

import org.joda.time.DateTime;

/**
 * Metadata about a Yammer metric
 */
public class YammerKey {
    public static final String NAME = "name";
    public static final String TYPE = "type";
    public static final String TIMESTAMP = "timestamp";
    public static final String GAUGE = "gauge";
    public static final String COUNTER = "counter";
    public static final String HISTOGRAM = "histogram";
    public static final String METER = "meter";
    public static final String TIMER = "timer";
    public static final String VALUE = "value";
    public static final String COUNT = "count";
    public static final String MAX = "max";
    public static final String MEAN = "mean";
    public static final String MIN = "min";
    public static final String STDDEV = "stddev";
    public static final String P_50 = "p50";
    public static final String P_75 = "p75";
    public static final String P_95 = "p95";
    public static final String P_98 = "p98";
    public static final String P_99 = "p99";
    public static final String P_999 = "p999";
    public static final String ONE_MINUTE_RATE = "one-minute-rate";
    public static final String FIVE_MINUTE_RATE = "five-minute-rate";
    public static final String FIFTEEN_MINUTE_RATE = "fifteen-minute-rate";
    public static final String MEAN_RATE = "mean-rate";
    public static final String TOPIC = "yammer";

    private String name;
    private String type;
    private DateTime timestamp;

    public String getName() {
        return name;
    }

    public void setName(final String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(final String type) {
        this.type = type;
    }

    public DateTime getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(final DateTime timestamp) {
        this.timestamp = timestamp;
    }
}
