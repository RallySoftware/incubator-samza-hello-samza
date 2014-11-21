package samza.examples.yammer;

import com.codahale.metrics.*;
import com.codahale.metrics.Timer.Context;

import java.util.Random;
import java.util.concurrent.TimeUnit;

public class Tester {
    public static void main(String... args) {
        final Random random = new Random();

        MetricRegistry metrics = new MetricRegistry();
        SamzaReporter reporter = new SamzaReporter(metrics, "samza-tester", null, TimeUnit.SECONDS, TimeUnit.MILLISECONDS, "localhost:9092");
        reporter.start(15, TimeUnit.SECONDS);

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

        while (true) {
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
}
