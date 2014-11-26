package z.influx;

import org.joda.time.DateTime;

public class InfluxKey {
    private String serie;
    private DateTime timestamp;

    public DateTime getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(final DateTime timestamp) {
        this.timestamp = timestamp;
    }

    public String getSerie() {
        return serie;
    }

    public void setSerie(final String serie) {
        this.serie = serie;
    }
}
