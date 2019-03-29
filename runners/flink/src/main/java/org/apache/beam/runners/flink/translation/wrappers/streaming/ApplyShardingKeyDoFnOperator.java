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
package org.apache.beam.runners.flink.translation.wrappers.streaming;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.ShardedKeyCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.io.WriteFiles;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.ShardedKey;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;

public class ApplyShardingKeyDoFnOperator<UserT>
    extends DoFnOperator<UserT, KV<ShardedKey<Integer>, UserT>> {

  private final boolean useAutoBalancedSharding;

  public ApplyShardingKeyDoFnOperator(
      WriteFiles.ApplyShardingKeyFn doFn,
      String stepName,
      Coder<WindowedValue<UserT>> inputWindowedCoder,
      Coder<UserT> inputCoder,
      Map<TupleTag<?>, Coder<?>> outputCoders,
      TupleTag<KV<ShardedKey<Integer>, UserT>> mainOutputTag,
      List<TupleTag<?>> additionalOutputTags,
      OutputManagerFactory<KV<ShardedKey<Integer>, UserT>> outputManagerFactory,
      WindowingStrategy<?, ?> windowingStrategy,
      Map<Integer, PCollectionView<?>> sideInputTagMapping,
      Collection<PCollectionView<?>> sideInputs,
      PipelineOptions options,
      Coder<?> keyCoder,
      KeySelector<WindowedValue<UserT>, ?> keySelector) {
    super(
        doFn,
        stepName,
        inputWindowedCoder,
        inputCoder,
        outputCoders,
        mainOutputTag,
        additionalOutputTags,
        outputManagerFactory,
        windowingStrategy,
        sideInputTagMapping,
        sideInputs,
        options,
        keyCoder,
        keySelector);

    FlinkPipelineOptions flinkOptions = options.as(FlinkPipelineOptions.class);
    useAutoBalancedSharding = flinkOptions.isAutoBalancedShardingEnabled();
  }

  @Override
  protected DoFn<UserT, KV<ShardedKey<Integer>, UserT>> getDoFn() {

    if (useAutoBalancedSharding) {

      int parallelism = getRuntimeContext().getNumberOfParallelSubtasks();
      int maxParallelism = getRuntimeContext().getMaxNumberOfParallelSubtasks();

      AutoBalancedKeyGenerator generator = new AutoBalancedKeyGenerator(parallelism, maxParallelism);

      int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();

      ((WriteFiles.ApplyShardingKeyFn)doFn).setShardKeyFactory(new WriteFiles.ShardKeyFactory() {
        @Override
        public ShardedKey<Integer> createShardKey(Integer key, int shardCount) {
          int targetShard = subtaskIndex % shardCount;
          return generator.generate(key, targetShard);
        }
      });
    }
    return doFn;
  }


  private static class AutoBalancedKeyGenerator {

    private int parallelism;
    private int maxParallelism;

    private AutoBalancedKeyGenerator(int parallelism, int maxParallelism) {
      this.parallelism = parallelism;
      this.maxParallelism = maxParallelism;
    }

    private transient Map<CacheKey, ShardedKey<Integer>> cache = new HashMap<>();
    private transient Set<Integer> usedSalts = new HashSet<>();

    public ShardedKey<Integer> generate(int key, int targetShard) {

      if (targetShard < 0) {
        throw new IllegalArgumentException("Shard number must not be negative");
      }

      if (cache.size() < targetShard) {
        for (int i = 0; i < targetShard; i++) {
          generateInternal(new CacheKey(key, i));
        }
      }

      return generateInternal(new CacheKey(key, targetShard));
    }

    private ShardedKey<Integer> generateInternal(CacheKey key) {

      ShardedKey<Integer> result = cache.get(key);
      if (result != null) {
        return result;
      }

      int salt = -1;
      while (true) {

        salt++;
        ShardedKey<Integer> shk = ShardedKey.of(key.key, key.shard, salt);

        int targetPartition = key.shard % parallelism;

        ByteBuffer effectiveKey;
        try {
          byte[] bytes = CoderUtils.encodeToByteArray(ShardedKeyCoder.of(VarIntCoder.of()), shk);
          effectiveKey = ByteBuffer.wrap(bytes);
        } catch (CoderException e) {
          throw new RuntimeException(e);
        }

        int partition = KeyGroupRangeAssignment.assignKeyToParallelOperator(effectiveKey, maxParallelism, parallelism);

        if (partition == targetPartition && !usedSalts.contains(salt)) {
          usedSalts.add(salt);
          cache.put(key, shk);
          return shk;
        }
      }
    }

    private void writeObject(ObjectOutputStream stream) throws IOException {
      stream.writeInt(parallelism);
      stream.writeInt(maxParallelism);
    }

    private void readObject(ObjectInputStream stream) throws IOException, ClassNotFoundException {
      parallelism = stream.readInt();
      maxParallelism = stream.readInt();
      cache = new HashMap<>();
      usedSalts = new HashSet<>();
    }

    private static class CacheKey {
      private final int key;
      private final int shard;
      private final int code;

      private CacheKey(int key, int shard) {
        this.key = key;
        this.shard = shard;

        int result = 1;
        result = 31 * result + key;
        result = 31 * result + shard;
        code = result;
      }

      @Override
      public int hashCode() {
        return code;
      }

      @Override
      public boolean equals(Object obj) {
        if (obj instanceof CacheKey) {
          CacheKey o = (CacheKey) obj;
          return o.key == key && o.shard == shard;
        }
        return false;
      }
    }
  }
}
