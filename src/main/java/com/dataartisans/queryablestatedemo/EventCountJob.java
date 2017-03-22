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

import org.apache.flink.api.common.state.FoldingStateDescriptor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.QueryableStateOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.FlinkMiniCluster;
import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * A job generating {@link BumpEvent} instances and counting them.
 *
 * <p>The generated events are keyed by their item ID and then counted. The
 * count is maintained via Flink's managed state as exposed for external
 * queries.
 *
 * <p>Checkout the {@link EventCountClient} for the querying part.
 */
public class EventCountJob {

  /**
   * External name for the state instance. The count can be queried under this name.
   */
  public final static String ITEM_COUNTS = "itemCounts";

  public static void main(String[] args) throws Exception {
    ParameterTool params = ParameterTool.fromArgs(args);
    final boolean printThroughput = params.getBoolean("printThroughput", true);
    final int port = params.getInt("port", 6124);
    final int parallelism = params.getInt("parallelism", 4);

    // We use a mini cluster here for sake of simplicity, because I don't want
    // to require a Flink installation to run this demo. Everything should be
    // contained in this JAR.

    Configuration config = new Configuration();
    config.setInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, port);
    config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, 1);
    config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, parallelism);
    // In a non MiniCluster setup queryable state is enabled by default.
    config.setBoolean(QueryableStateOptions.SERVER_ENABLE, true);

    FlinkMiniCluster flinkCluster = new LocalFlinkMiniCluster(config, false);
    try {
      flinkCluster.start(true);

      StreamExecutionEnvironment env = StreamExecutionEnvironment
          .createRemoteEnvironment("localhost", port, parallelism);

      DataStream<BumpEvent> bumps = env
          .addSource(new BumpEventGeneratorSource(printThroughput));

      // Increment the count for each event (keyed on itemId)
      FoldingStateDescriptor<BumpEvent, Long> countingState = new FoldingStateDescriptor<>(
          ITEM_COUNTS,
          0L,             // Initial value is 0
          (acc, event) -> acc + 1L, // Increment for each event
          Long.class);

      bumps.keyBy(BumpEvent::getItemId)
          .asQueryableState(ITEM_COUNTS, countingState);

      JobGraph jobGraph = env.getStreamGraph().getJobGraph();

      System.out.println("[info] Job ID: " + jobGraph.getJobID());
      System.out.println();

      flinkCluster.submitJobAndWait(jobGraph, false);
    } finally {
      flinkCluster.shutdown();
    }
  }

}
