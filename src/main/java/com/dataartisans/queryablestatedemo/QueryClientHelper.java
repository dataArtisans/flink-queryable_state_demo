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

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.query.QueryableStateClient;
import org.apache.flink.runtime.query.netty.UnknownKeyOrNamespace;
import org.apache.flink.runtime.query.netty.message.KvStateRequestSerializer;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.util.DataInputDeserializer;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

/**
 * This is a wrapper around Flink's {@link QueryableStateClient} (as of Flink
 * 1.2.0) that hides the low-level type serialization details.
 *
 * <p>Queries are executed synchronously via {@link #queryState(String, Object)}.
 *
 * @param <K> Type of the queried keys
 * @param <V> Type of the queried values
 */
public class QueryClientHelper<K, V> implements AutoCloseable {

  /**
   * ID of the job to query.
   */
  private final JobID jobId;

  /**
   * Serializer for the keys.
   */
  private final TypeSerializer<K> keySerializer;

  /**
   * Serializer for the result values.
   */
  private final TypeSerializer<V> valueSerializer;

  /**
   * Timeout for each query. After this timeout, the query fails with a {@link TimeoutException}.
   */
  private final FiniteDuration queryTimeout;

  /**
   * The wrapper low-level {@link QueryableStateClient}.
   */
  private final QueryableStateClient client;

  /**
   * Creates the queryable state client wrapper.
   *
   * @param jobManagerHost Host for JobManager communication
   * @param jobManagerPort Port for JobManager communication.
   * @param jobId ID of the job to query.
   * @param keySerializer Serializer for keys.
   * @param valueSerializer Serializer for returned values.
   * @param queryTimeout Timeout for queries.
   * @throws Exception Thrown if creating the {@link QueryableStateClient} fails.
   */
  QueryClientHelper(
      String jobManagerHost,
      int jobManagerPort,
      JobID jobId,
      TypeSerializer<K> keySerializer,
      TypeSerializer<V> valueSerializer,
      Time queryTimeout) throws Exception {

    this.jobId = jobId;
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
    this.queryTimeout = new FiniteDuration(queryTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);

    Configuration config = new Configuration();
    config.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, jobManagerHost);
    config.setInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, jobManagerPort);

    this.client = new QueryableStateClient(config);
  }

  /**
   * Queries a state instance for the key.
   *
   * @param name Name of the state instance to query. This is the external name as given to {@link
   * org.apache.flink.api.common.state.StateDescriptor#setQueryable(String)} or {@link
   * org.apache.flink.streaming.api.datastream.KeyedStream#asQueryableState(String)}.
   * @param key The key to query
   * @return The returned value if it is available
   */
  Optional<V> queryState(String name, K key) throws Exception {
    if (name == null) {
      throw new NullPointerException("Name");
    }

    if (key == null) {
      throw new NullPointerException("Key");
    }

    // Serialize the key. The namespace is ignored as it's only relevant for
    // windows which are not yet exposed for queries.
    byte[] serializedKey = KvStateRequestSerializer.serializeKeyAndNamespace(
        key,
        keySerializer,
        VoidNamespace.INSTANCE,
        VoidNamespaceSerializer.INSTANCE);

    // Submit the query
    Future<byte[]> queryFuture = client.getKvState(jobId, name, key.hashCode(), serializedKey);

    try {
      // Wait for the result
      byte[] queryResult = Await.result(queryFuture, queryTimeout);

      DataInputDeserializer dis = new DataInputDeserializer(
          queryResult,
          0,
          queryResult.length);

      V value = valueSerializer.deserialize(dis);

      return Optional.ofNullable(value);
    } catch (UnknownKeyOrNamespace e) {
      // The future is failed with this Exception if the key does not exist
      return Optional.empty();
    }
  }

  @Override
  public void close() throws Exception {
    client.shutDown();
  }

}
