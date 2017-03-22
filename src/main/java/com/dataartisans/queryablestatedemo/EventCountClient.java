/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dataartisans.queryablestatedemo;

import java.io.PrintWriter;
import java.util.Optional;
import jline.console.ConsoleReader;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;

public class EventCountClient {

  public static void main(String[] args) throws Exception {
    if (args.length == 0) {
      throw new IllegalArgumentException("Missing required job ID argument. "
          + "Usage: ./EventCountClient <jobID> [jobManagerHost] [jobManagerPort]");
    }
    String jobIdParam = args[0];

    // Configuration
    final String jobManagerHost = args.length > 1 ? args[1] : "localhost";
    final int jobManagerPort = args.length > 1 ? Integer.parseInt(args[1]) : 6124;

    System.out.println("Using JobManager " + jobManagerHost + ":" + jobManagerPort);

    final JobID jobId = JobID.fromHexString(jobIdParam);

    final StringSerializer keySerializer = StringSerializer.INSTANCE;
    final LongSerializer valueSerializer = LongSerializer.INSTANCE;

    final Time queryTimeout = Time.seconds(5);

    try (
        // This helper is for convenience and not part of Flink
        QueryClientHelper<String, Long> client = new QueryClientHelper<>(
            jobManagerHost,
            jobManagerPort,
            jobId,
            keySerializer,
            valueSerializer,
            queryTimeout)) {

      printUsage();

      ConsoleReader reader = new ConsoleReader();
      reader.setPrompt("$ ");

      PrintWriter out = new PrintWriter(reader.getOutput());

      String line;
      while ((line = reader.readLine()) != null) {
        String key = line.toLowerCase().trim();
        out.printf("[info] Querying key '%s'\n", key);

        try {
          long start = System.currentTimeMillis();
          Optional<Long> count = client.queryState(EventCountJob.ITEM_COUNTS, key);
          long end = System.currentTimeMillis();

          long duration = Math.max(0, end - start);

          if (count.isPresent()) {
            out.printf("%d (query took %d ms)\n", count.get(), duration);
          } else {
            out.printf("Unknown key %s (query took %d ms)\n", key, duration);
          }
        } catch (Exception e) {
          out.println("Query failed because of the following Exception:");
          e.printStackTrace(out);
        }
      }
    }
  }

  private static void printUsage() {
    System.out.println("Enter a key to query.");
    System.out.println();
    System.out.println("The EventCountJob " + EventCountJob.ITEM_COUNTS + " state instance "
        + "has String keys that are three characters long and alphanumeric, e.g. 'AP2' or 'LOL'.");
    System.out.println();
  }

}
