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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import jline.console.ConsoleReader;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.FoldingState;
import org.apache.flink.api.common.state.FoldingStateDescriptor;
import org.apache.flink.queryablestate.client.QueryableStateClient;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;

public class EventCountClient {

	public static void main(String[] args) throws Exception {
		if (args.length == 0) {
			throw new IllegalArgumentException("Missing required job ID argument. "
					+ "Usage: ./EventCountClient <jobID>");
		}
		String jobIdParam = args[0];

		// configuration
		final JobID jobId = JobID.fromHexString(jobIdParam);
		final String jobManagerHost = "localhost";
		final int jobManagerPort = 9069;

		QueryableStateClient client = new QueryableStateClient(jobManagerHost, jobManagerPort);
		client.setExecutionConfig(new ExecutionConfig());

		// state descriptor for the state to be fetched
		FoldingStateDescriptor<BumpEvent, Long> countingState = new FoldingStateDescriptor<>(
				EventCountJob.ITEM_COUNTS,
				0L,             // Initial value is 0
				(acc, event) -> acc + 1L, // Increment for each event
				Long.class);

		System.out.println("Using JobManager " + jobManagerHost + ":" + jobManagerPort);
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

				CompletableFuture<FoldingState<BumpEvent, Long>> resultFuture =
						client.getKvState(jobId, EventCountJob.ITEM_COUNTS, key, BasicTypeInfo.STRING_TYPE_INFO, countingState);

				resultFuture.thenAccept(response -> {
					try {
						Long count = response.get();
						long end = System.currentTimeMillis();
						long duration = Math.max(0, end - start);

						if (count != null) {
							out.printf("%d (query took %d ms)\n", count, duration);
						} else {
							out.printf("Unknown key %s (query took %d ms)\n", key, duration);
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
				});

				resultFuture.get(5, TimeUnit.SECONDS);

			} catch (Exception e) {
				e.printStackTrace();
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
