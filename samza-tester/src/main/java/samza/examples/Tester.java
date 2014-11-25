/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package samza.examples;

import com.codahale.metrics.*;
import com.codahale.metrics.Timer.Context;
import org.apache.samza.config.factories.PropertiesConfigFactory;
import samza.examples.reporter.SamzaReporter;

import java.util.Random;
import java.util.concurrent.TimeUnit;

public class Tester {
    public static final String[] LIPSUM;

    static {
        String lipsum = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Praesent pulvinar hendrerit magna. Donec viverra mi ut augue vehicula egestas. Proin quis tincidunt elit. Donec eu enim ullamcorper, facilisis risus eu, sollicitudin velit. Nulla eget lorem leo. In pulvinar elit nec dolor dapibus, eleifend blandit est lacinia. Vivamus euismod faucibus ligula nec tincidunt. Interdum et malesuada fames ac ante ipsum primis in faucibus. Mauris placerat nec neque id posuere. Phasellus nec tortor nibh. Praesent finibus est at sem luctus bibendum in in ipsum. Cras in suscipit mauris. Duis ligula nibh, convallis sed dui dignissim, tincidunt suscipit dolor. Cras eget erat viverra, dictum augue vel, iaculis purus. Aliquam luctus sem ipsum, a pellentesque ligula pulvinar eu. Morbi vel lectus eget arcu finibus lacinia vel sit amet arcu. Maecenas molestie mollis velit eget mattis. Fusce ut auctor felis. Maecenas dictum orci a diam auctor, non pulvinar lacus placerat. Pellentesque habitant morbi tristique senectus et netus et malesuada fames ac turpis egestas. Proin sit amet maximus tellus, sed cursus lorem. Nam sed gravida ante. Maecenas maximus maximus purus. Maecenas non mollis nisi. Cras aliquet ligula et erat finibus, dapibus viverra turpis fringilla. Aenean nec nulla egestas, bibendum magna ut, blandit nulla. Integer magna eros, pretium a lorem sit amet, venenatis blandit tortor. Proin ex dolor, pharetra nec molestie ac, tempus nec diam. Proin tincidunt leo eu diam tincidunt imperdiet. Proin varius eu augue sed venenatis. Aliquam bibendum id quam imperdiet porta. Praesent vitae volutpat neque. Duis vel ante quis est efficitur consectetur. Vivamus scelerisque fringilla tortor, eu malesuada turpis porttitor vel. Fusce cursus, orci at scelerisque condimentum, augue risus elementum elit, non gravida libero nisi at arcu. Nunc scelerisque gravida urna sed tempor. Nunc sit amet enim vitae massa tincidunt semper. Sed quis hendrerit velit. Vivamus fermentum mi vitae quam maximus, ut venenatis arcu porta. Fusce laoreet ac elit sit amet mattis. Fusce laoreet erat vitae urna ultricies, ac tincidunt sem convallis. Phasellus at neque lorem. Pellentesque habitant morbi tristique senectus et netus et malesuada fames ac turpis egestas. Nunc quis lorem nibh. Donec efficitur, arcu eu varius molestie, sapien nulla imperdiet enim, sed sodales nunc dolor quis orci. Pellentesque habitant morbi tristique senectus et netus et malesuada fames ac turpis egestas. Suspendisse eu congue leo. Nam venenatis mi sed lorem laoreet, eu fermentum elit rutrum. Sed elementum egestas felis nec rutrum. Suspendisse ut imperdiet libero. Sed suscipit metus id ex vulputate vulputate. Donec congue, erat nec volutpat venenatis, lacus orci placerat orci, sed faucibus enim erat ut risus. Praesent sit amet porta risus, ut tristique velit. Pellentesque quis feugiat leo. Nam condimentum, nunc non imperdiet vestibulum, nunc lorem tincidunt urna, quis maximus ante massa et sapien. Curabitur sit amet ultrices dolor. Aenean ac sapien in ligula fermentum finibus eu tincidunt sem. Sed quis odio eu ante iaculis tempor. Maecenas posuere, felis sodales mollis facilisis, urna nibh viverra sapien, finibus fermentum urna massa at eros. Etiam leo libero, maximus vitae pretium ac, rutrum et nulla. Aliquam erat volutpat. Duis bibendum ullamcorper quam.";
        LIPSUM = lipsum.split("[^\\w]+");
    }

    public static void main(String... args) throws InterruptedException {
        final Random random = new Random();

        MetricRegistry metrics = new MetricRegistry();
        SamzaReporter reporter = SamzaReporter.forRegistry(metrics)
                                              .named("samza-tester")
                                              .withBrokerList("localhost:9092")
                                              .build();
        reporter.start(500, TimeUnit.MILLISECONDS);

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

        org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger("samza.tester");

        PropertiesConfigFactory

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

            StringBuilder builder = new StringBuilder();
            for (int i = 0; i < x; i++) {
                builder.append(LIPSUM[random.nextInt(LIPSUM.length)]);
                builder.append(" ");
            }

            int level = random.nextInt(6);
            switch (level) {
                case 0:
                    logger.trace(builder);
                    break;

                case 1:
                    logger.debug(builder);
                    break;

                case 2:
                    logger.info(builder);
                    break;

                case 3:
                    logger.warn(builder);
                    break;

                case 4:
                    logger.error(builder);
                    break;

                case 5:
                    logger.fatal(builder);
                    break;
            }
        }
    }
}
