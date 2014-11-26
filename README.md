Samza
===

Real-time log and metric processing via Kafka and Samza.

Getting Started
---

### Get the code:

Clone the repo:

    # cd projects
    # git clone https://github.com/rallysoftware/incubator-samza-hello-samza
    # cd incubator-samza-hello-samza

Create a deployment target for development and testing:

    # mkdir deploy

### Install and start all of the required infrastructure:

Install and start Zookeeper, Kafka, YARN, InfluxDB, Elasticsearch, and a stub Splunk.  Each service is "deployed" into a separate directory under `deploy/`.

    # ./bin/grid install all
    # ./bin/grid start all

### Build, deploy, and run the code

Build two artifacts: the deployable tarball for Samza in `samza-job-package/target/samza-job-package-0.7.0-dist.tar.gz` and a tester program in `samza-tester/target/samza-tester-0.7.0-shaded.jar`.

    # mvn clean package

"Deploy" the Samza application to the `deploy/` directory.

    # mkdir -p deploy/samza
    # tar -xvzf samza-job-package/target/samza-job-package-0.7.0-dist.tar.gz -C deploy/samza

Launch all of the stream tasks to the local YARN cluster.

    # deploy/samza/bin/run-job.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory --config-path=file://$PWD/deploy/samza/config/jarvis.properties
    # deploy/samza/bin/run-job.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory --config-path=file://$PWD/deploy/samza/config/log4j.properties
    # deploy/samza/bin/run-job.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory --config-path=file://$PWD/deploy/samza/config/yammer.properties

Running tasks can be viewed at [http://localhost:8088/cluster](http://localhost:8088/cluster).  To stop a task, find the application ID in the interface and run:

    # ./deploy/samza/bin/kill-yarn-job.sh <APPLICATION_ID>

Infrastructure
---

### Zookeeper

Zookeeper is a distributed consistent store and a dependency for Kafka.  Version 3.4.3 is installed.

It can be started and stopped with the `grid` script:

    # ./bin/grid start zookeeper
    # ./bin/grid stop zookeeper

### Kafka

Kafka is the main store of log data.  Version 0.8.1.1 is installed.

It can be started and stopped with the `grid` script:

    # ./bin/grid start kafka
    # ./bin/grid stop kafka

### YARN

YARN is Hadoop 2.0, which is what Samza runs on.  Version 2.2.0 is installed.

It can be started and stopped with the `grid` script:

    # ./bin/grid start yarn
    # ./bin/grid stop yarn

### InfluxDB

InfluxDB is a time-series database.  Both log messages and metrics are written to it.  It is installed via `brew`.

It can be started (but not stopped) with the `grid` script:

    # ./bin/grid start influx

### Elasticsearch

Elasticsearch is a searchable document store based on Apache Lucene.  It is used as a shared store for metadata.  Version 1.4.0 is installed.

It can be started (but not stopped) with the `grid` script:

    # ./bin/grid start elasticsearch

### Splunk

Since writing to Splunk for real requires some server side hackery, the `grid` script sets up a netcat socket listening on port 9998 which dumps all of its output to `deploy/splunk/logs/splunk.log`.

It can be started (but not stopped) with the `grid` script:

    # ./bin/grid start splunk

Appender
---

A Log4J appender which writes directly to Kafka in a format that can be processed.  Writes to the "log4j" topic with a JSON object key and a string value.  The message should be "unformatted": all of the information such as source, timestamp, level, etc. is contained in the key and does not need to be replicated in the body.

### Example

#### Key:

    {
      "service": "my-service",                # The name of your service
      "log": "startup-log",                   # The name of the logger
      "host": "bld-service-01.f4tech.com",    # The host this message originated from
      "timestamp": "2014-01-01T00:00:00.000Z" # The time this message was created
    }

#### Message:

    Initializing service 'my-service'

#### `log4j.properties`:

    # The Samza appender should *NOT* be your root logger
    log4j.rootLogger=INFO, CONSOLE

    # Add formatting on the console
    log4j.appender.CONSOLE=org.apache.log4j.ConsoleAppender
    log4j.appender.CONSOLE.layout=org.apache.log4j.PatternLayout
    log4j.appender.CONSOLE.layout.ConversionPattern=[%t] %-5p %c - %m%n

    # Set up the Samza appender.  There are no "layout" properties
    log4j.appender.kafkaAppender=z.appender.SamzaAppender
    log4j.appender.kafkaAppender.BrokerList=localhost:9092
    log4j.appender.kafkaAppender.Service=my-service
    log4j.appender.kafkaAppender.Host=localhost

    # Attach the appender
    log4j.logger.my.service=DEBUG, kafkaAppender
    # Add this or you will get an infinite loop on startup
    log4j.additivity.samza.tester=false

#### `src/main/java/my/service/Main.java`

    package my.service;

    import org.apache.log4j.Logger;

    class Main {
        public static void main(String... args) {
            // Initializer the appender
            Logger logger = Logger.getLogger("my.service");

            // Start writing logs
            logger.info("Initializing service 'my-service'");
        }
    }

Dump
---

A task that prints every message it sees to STDERR.  Can be useful for debugging.

Flowdock
---

Flowdock integration

### System

### Tasks

Influx
---

Influx integration

### System

### Task

Jarvis
---

Flowdock bot

### Task

Log4J
---

Log4J message processing

### Task

Reporter
---

A Yammer metrics reporter which writes directly to Kafka in a format that can be processed.  Writes to the "yammer" topic with a JSON object as a key and a JSON map as a value.  The key contains all of the metric metadata, such as its name, the host, the timestamp, etc., while the value contains all of the measurements.

### Example

#### Key:

    {
      "name":      "my.service.request-meter", # The name of this metric
      "type":      "meter",                    # The type of metric: "gauge", "counter", "meter", "timer", or "histogram"
      "timestamp": "2014-01-01T00:00:00.000Z"  # When the measurements were taken
    }

#### Gauge Value:

    {
      "value": "Some value" # Can be any JSON
    }

#### Counter Value:

    {
      "count": 42
    }

#### Meter Value:

    {
      "count": 42,
      "one-minute-rate": 41,
      "five-minute-rate": 40,
      "fifteen-minute-rate": 39,
      "mean-rate": 38
    }

#### Timer Value:

    {
      "max": 100,
      "mean": 50,
      "min": 0
      "stddev": 24,
      "p50": 50,
      "p75": 75,
      "p95": 95,
      "p98": 98,
      "p99": 99,
      "p999": 99.9
    }

#### Histogram Value:

    {
      "count": 42,
      "max": 100,
      "mean": 50,
      "min": 0
      "stddev": 24,
      "p50": 50,
      "p75": 75,
      "p95": 95,
      "p98": 98,
      "p99": 99,
      "p999": 99.9
    }

#### `src/main/java/my/service/Main.java`

    package my.service;

    import com.codahale.metrics.*;
    import com.codahale.metrics.Timer.Context;
    import z.reporter.SamzaReporter;

    import java.util.Random;
    import java.util.concurrent.TimeUnit;

    class Main {
        public static void main(String... args) {
            final Random random = new Random();

            // Create and start the reporter
            MetricRegistry metrics = new MetricRegistry();
            SamzaReporter reporter = SamzaReporter.forRegistry(metrics)
                                                  .named("my-service")
                                                  .withBrokerList("kafka-broker:9092")
                                                  .build();
            reporter.start(15, TimeUnit.SECONDS);

            // Register a few metrics
            metrics.register("test-gauge", new Gauge<Integer>() {
                @Override
                public Integer getValue() {
                    return random.nextInt(100);
                }
            });
            Counter testCounter = metrics.counter("test-counter");
            Histogram testHistogram = metrics.histogram("test-histogram");
            Timer testTimer = metrics.timer("test-timer");
            Meter testMeter = metrics.meter("test-meter");

            // Start writing metrics
            int x = random.nextInt(100);
            testCounter.inc(x);
            testHistogram.update(x);
            testMeter.mark(x);
            Context context = testTimer.time();
            try {
                Thread.sleep(x);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            context.stop();
        }
    }

Splunk
---

Splunk integration

### System

### Task

Yammer
---

Yammer metric processing

### Task
