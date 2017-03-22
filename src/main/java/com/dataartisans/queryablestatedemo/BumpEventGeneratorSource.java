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

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

/**
 * A (non-parallel) {@link BumpEvent} generator source.
 */
class BumpEventGeneratorSource extends RichSourceFunction<BumpEvent> {

  private static final long serialVersionUID = 244478060020939187L;

  /**
   * Number of alpha numeric characters of the items.
   */
  private static final int ITEM_ID_NUM_CHARS = 3;

  /**
   * Flag indicating whether we should print the throughput. If true, the {@link ThroughputLogger}
   * thread is started.
   */
  private final boolean printThroughput;

  /**
   * Flag indicating whether we are still running.
   */
  private volatile boolean running = true;

  BumpEventGeneratorSource(boolean printThroughput) {
    this.printThroughput = printThroughput;
  }

  @Override
  public void run(SourceContext<BumpEvent> sourceContext) throws Exception {
    final Random rand = new Random();
    final AtomicLong count = new AtomicLong();

    Thread throughputLogger = null;
    if (printThroughput) {
      throughputLogger = new Thread(new ThroughputLogger(count), "ThroughputLogger");
      throughputLogger.start();
    }

    try {
      while (running) {
        // Generate random events
        final int userId = rand.nextInt(Integer.MAX_VALUE);

        final String itemCase = RandomStringUtils
            .randomAlphanumeric(ITEM_ID_NUM_CHARS).toLowerCase();

        synchronized (sourceContext.getCheckpointLock()) {
          sourceContext.collect(new BumpEvent(userId, itemCase));
        }

        // Increment count for throughput logger
        count.incrementAndGet();

        Thread.yield();
      }
    } finally {
      if (throughputLogger != null) {
        throughputLogger.interrupt();
        throughputLogger.join();
      }
    }
  }

  @Override
  public void cancel() {
    running = false;
  }

  // --------------------------------------------------------------------------

  private static class ThroughputLogger implements Runnable {

    private final AtomicLong count;

    ThroughputLogger(AtomicLong count) {
      this.count = count;
    }

    @Override
    public void run() {
      long lastCount = 0L;
      long lastTimestamp = System.currentTimeMillis();

      while (!Thread.interrupted()) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          return;
        }

        final long ts = System.currentTimeMillis();

        final long currCount = count.get();

        final double factor = (ts - lastTimestamp) / 1000.0;
        final int perSec = (int) ((currCount - lastCount) / factor);

        lastTimestamp = ts;
        lastCount = currCount;

        System.out.println(String.format("Generating %d elements per second", perSec));
      }
    }
  }

}
