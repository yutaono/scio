/*
 * Copyright 2017 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.scio.bigtable;

import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.common.cache.Cache;
import com.google.common.collect.Maps;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

// Decorate input element A with Bigtable lookup result B
public abstract class BlockingBigtableDoFn<A, B> extends DoFn<A, KV<A, B>> {
  private static final Logger LOG = LoggerFactory.getLogger(BlockingBigtableDoFn.class);
  private static final ConcurrentMap<UUID, BigtableSession> session = Maps.newConcurrentMap();
  private static final ConcurrentMap<UUID, Cache> cache = Maps.newConcurrentMap();

  // DoFn is deserialized once per CPU core. This ensures all cores share the same BigtableSession
  // and Cache.
  private final UUID uuid;

  private final BigtableOptions options;
  private final SerializableSupplier<Cache<A, B>> cacheSupplier;

  // Method to perform Bigtable lookup from input A to result B
  public abstract B processElement(BigtableSession session, A input);

  public BlockingBigtableDoFn(BigtableOptions options) {
    this(options, () -> null);
  }

  public BlockingBigtableDoFn(BigtableOptions options,
                              SerializableSupplier<Cache<A, B>> cacheSupplier) {
    this.uuid = UUID.randomUUID();
    this.options = options;
    this.cacheSupplier = cacheSupplier;
  }

  @SuppressWarnings("unchecked")
  private B cacheGet(A key) {
    Cache<A, B> c = cache.get(this.uuid);
    return c == null ? null : c.getIfPresent(key);
  }

  @SuppressWarnings("unchecked")
  private void cachePut(A key, B value) {
    Cache<A, B> c = cache.get(this.uuid);
    if (c != null) {
      c.put(key, value);
    }
  }

  @Setup
  public void setup() {
    LOG.info("Setup for {}", this);
    session.computeIfAbsent(uuid, uuid -> {
      LOG.info("Creating BigtableSession with BigtableOptions {}", options);
      try {
        return new BigtableSession(options);
      } catch (IOException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    });
    cache.computeIfAbsent(uuid, uuid -> cacheSupplier.get());
  }

  @SuppressWarnings("unchecked")
  @ProcessElement
  public void processElement(ProcessContext c, BoundedWindow window) {
    final A input = c.element();
    B cached = cacheGet(input);
    if (cached != null) {
      c.output(KV.of(input, cached));
      return;
    }

    B output = processElement(session.get(uuid), input);
    cachePut(input, output);
    c.output(KV.of(input, output));
  }

  public interface SerializableSupplier<T> extends Supplier<T>, Serializable {}

}
