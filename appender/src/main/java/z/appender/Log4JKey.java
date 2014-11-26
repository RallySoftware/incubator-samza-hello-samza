package z.appender;

import org.joda.time.DateTime;

/**
 * Metadata about a Log4J message
 */
public class Log4JKey {
    public static final String SERVICE = "service";
    public static final String LOG = "log";
    public static final String HOST = "host";
    public static final String TIMESTAMP = "timestamp";
    private String service;
    private String log;
    private String host;
    private DateTime timestamp;

    public String getService() {
        return service;
    }

    public void setService(final String service) {
        this.service = service;
    }

    public String getLog() {
        return log;
    }

    public void setLog(final String log) {
        this.log = log;
    }

    public String getHost() {
        return host;
    }

    public void setHost(final String host) {
        this.host = host;
    }

    public DateTime getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(final DateTime timestamp) {
        this.timestamp = timestamp;
    }
}
