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
package org.apache.beam.sdk.values;

import java.io.Serializable;
import java.util.Objects;

/** A key and a shard number. */
public class ShardedKey<K> implements Serializable {
  private static final long serialVersionUID = 1L;
  private final K key;
  private final int shardNumber;
  private final int salt;

  public static <K> ShardedKey<K> of(K key, int shardNumber) {
    return of(key, shardNumber, 0);
  }

  public static <K> ShardedKey<K> of(K key, int shardNumber, int salt) {
    return new ShardedKey<>(key, shardNumber, salt);
  }

  private ShardedKey(K key, int shardNumber, int salt) {
    this.key = key;
    this.shardNumber = shardNumber;
    this.salt = salt;
  }

  public K getKey() {
    return key;
  }

  public int getShardNumber() {
    return shardNumber;
  }

  public int getSalt() {
    return salt;
  }

  @Override
  public String toString() {
    return "key: " + key + " shard: " + shardNumber + " salt: " + salt;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof ShardedKey)) {
      return false;
    }
    ShardedKey<K> other = (ShardedKey<K>) o;
    return Objects.equals(key, other.key)
        && Objects.equals(shardNumber, other.shardNumber)
        && Objects.equals(salt, other.salt);
  }

  @Override
  public int hashCode() {
    return Objects.hash(key, shardNumber, salt);
  }
}
