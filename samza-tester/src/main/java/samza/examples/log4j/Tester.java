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

package samza.examples.log4j;

public class Tester {

    public static final int sleep = 50;

    public static void main(String... args) throws InterruptedException {
        int x = 0;
        int sleep = Integer.getInteger("sleep", 0);
        org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger("samza.tester");

        while (true) {
            x++;

            Thread.sleep(sleep);
            logger.trace("This is trace message #" + x);

            Thread.sleep(sleep);
            logger.debug("This is debug message #" + x);

            Thread.sleep(sleep);
            logger.info("This is info message #" + x);

            Thread.sleep(sleep);
            logger.warn("This is warn message #" + x);

            Thread.sleep(sleep);
            logger.error("This is error message #" + x);

            Thread.sleep(sleep);
            logger.fatal("This is fatal message #" + x);
        }
    }
}
