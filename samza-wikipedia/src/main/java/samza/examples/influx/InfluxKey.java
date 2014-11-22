package samza.examples.influx;

import org.joda.time.DateTime;

public class InfluxKey {
    private DateTime timestamp;

    public DateTime getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(final DateTime timestamp) {
        this.timestamp = timestamp;
    }
}
