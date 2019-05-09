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
package org.apache.beam.runners.flink;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertNull;

import java.util.List;
import java.util.UUID;
import org.apache.beam.runners.flink.FlinkStreamingPipelineTranslator.FlinkAutoBalancedShardKeyShardingFunction;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.ShardedKey;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Lists;
import org.junit.Test;

/** Tests if overrides are properly applied. */
public class FlinkStreamingPipelineTranslatorTest {

  @Test
  public void testAutoBalanceShardKeyResolvesMaxParallelism() {

    assertThat(
        new FlinkAutoBalancedShardKeyShardingFunction<>(3, -1, StringUtf8Coder.of())
            .getMaxParallelism(),
        equalTo(128));
    assertThat(
        new FlinkAutoBalancedShardKeyShardingFunction<>(3, 0, StringUtf8Coder.of())
            .getMaxParallelism(),
        equalTo(128));
  }

  @Test
  public void testAutoBalanceShardKeyCacheIsNotSerialized() throws Exception {

    FlinkAutoBalancedShardKeyShardingFunction<String, String> fn =
        new FlinkAutoBalancedShardKeyShardingFunction<>(2, 2, StringUtf8Coder.of());

    assertNull(fn.getCache());

    fn.assignShardKey("target/destination1", "one", 10);
    fn.assignShardKey("target/destination2", "two", 10);

    assertThat(fn.getCache().size(), equalTo(2));
    assertThat(SerializableUtils.clone(fn).getCache(), nullValue());
  }

  @Test
  public void testAutoBalanceShardKeyCacheIsStable() throws Exception {

    FlinkAutoBalancedShardKeyShardingFunction<String, String> fn =
        new FlinkAutoBalancedShardKeyShardingFunction<>(2, 2, StringUtf8Coder.of());

    // let's make shard assignment deterministic
    fn.setShardNumber(0);

    assertAssignedShardKeys(
        fn,
        Lists.newArrayList(
            KV.of("target/destination/1", "one"),
            KV.of("target/destination/1", "two"),
            KV.of("target/destination/2", "one"),
            KV.of("target/destination/2", "two")),
        Lists.newArrayList(
            ShardedKey.of(1804118576, 1),
            ShardedKey.of(1804118574, 0),
            ShardedKey.of(1804118607, 1),
            ShardedKey.of(1804118605, 0)));

    // with different order of observing destination, which is part of a key construction, we
    // should get the same results
    assertAssignedShardKeys(
        fn,
        Lists.newArrayList(
            KV.of("target/destination/1", "one"),
            KV.of("target/destination/2", "two"),
            KV.of("target/destination/2", "one"),
            KV.of("target/destination/1", "two")),
        Lists.newArrayList(
            ShardedKey.of(1804118576, 1),
            ShardedKey.of(1804118605, 0),
            ShardedKey.of(1804118607, 1),
            ShardedKey.of(1804118574, 0)));
  }

  @Test
  public void testAutoBalanceShardKeyCacheMaxSize() throws Exception {

    FlinkAutoBalancedShardKeyShardingFunction<String, String> fn =
        new FlinkAutoBalancedShardKeyShardingFunction<>(2, 2, StringUtf8Coder.of());

    for (int i = 0; i < FlinkAutoBalancedShardKeyShardingFunction.CACHE_MAX_SIZE * 2; i++) {
      fn.assignShardKey(UUID.randomUUID().toString(), "one", 2);
    }

    assertThat(
        fn.getCache().size(), equalTo(FlinkAutoBalancedShardKeyShardingFunction.CACHE_MAX_SIZE));
  }

  private void assertAssignedShardKeys(
      FlinkAutoBalancedShardKeyShardingFunction<String, String> fn,
      List<KV<String, String>> inputs,
      List<ShardedKey<Integer>> results)
      throws Exception {

    int index = 0;
    for (KV<String, String> input : inputs) {
      assertThat(
          fn.assignShardKey(input.getKey(), input.getValue(), 2), equalTo(results.get(index)));
      index++;
    }
  }
}
