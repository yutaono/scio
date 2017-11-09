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
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.Serializable;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.function.Consumer;
import java.util.function.Supplier;

// Decorate input element A with Bigtable lookup result B
public abstract class BigtableDoFn<A, B> extends DoFn<A, KV<A, B>> {
  private static final Logger LOG = LoggerFactory.getLogger(BigtableDoFn.class);
  private static final ConcurrentMap<UUID, BigtableSession> session = Maps.newConcurrentMap();
  private static final ConcurrentMap<UUID, Cache> cache = Maps.newConcurrentMap();

  // DoFn is deserialized once per CPU core. This ensures all cores share the same BigtableSession
  // and Cache.
  private final UUID instanceId;

  private final BigtableOptions options;
  private final Semaphore semaphore;
  private final SerializableSupplier<Cache<A, B>> cacheSupplier;

  // Data structures for handling async requests
  private final ConcurrentMap<UUID, ListenableFuture<B>> futures = Maps.newConcurrentMap();
  private final ConcurrentLinkedQueue<Result> results = Queues.newConcurrentLinkedQueue();
  private final ConcurrentLinkedQueue<Throwable> errors = Queues.newConcurrentLinkedQueue();
  private long requestCount;
  private long resultCount;

  // Method to perform Bigtable lookup from input A to result B
  public abstract ListenableFuture<B> processElement(BigtableSession session, A input);

  public BigtableDoFn(BigtableOptions options) {
    this(options, 1000, () -> null);
  }

  public BigtableDoFn(BigtableOptions options,
                      int maxPendingRequests) {
    this(options, maxPendingRequests, () -> null);
  }

  public BigtableDoFn(BigtableOptions options,
                      int maxPendingRequests,
                      SerializableSupplier<Cache<A, B>> cacheSupplier) {
    this.instanceId = UUID.randomUUID();
    this.options = options;
    this.cacheSupplier = cacheSupplier;
    this.semaphore = new Semaphore(maxPendingRequests);
  }

  @SuppressWarnings("unchecked")
  private B cacheGet(A key) {
    Cache<A, B> c = cache.get(this.instanceId);
    return c == null ? null : c.getIfPresent(key);
  }

  @SuppressWarnings("unchecked")
  private void cachePut(A key, B value) {
    Cache<A, B> c = cache.get(this.instanceId);
    if (c != null) {
      c.put(key, value);
    }
  }

  @Setup
  public void setup() {
    LOG.info("Setup for {}", this);
    session.computeIfAbsent(instanceId, instanceId -> {
      LOG.info("Creating BigtableSession with BigtableOptions {}", options);
      try {
        return new BigtableSession(options);
      } catch (IOException e) {
        e.printStackTrace();
        LOG.error("Failed to create BigtableSession", e);
        throw new RuntimeException("Failed to create BigtableSession", e);
      }
    });
    cache.computeIfAbsent(instanceId, instanceId -> cacheSupplier.get());
  }

  @StartBundle
  public void startBundle() {
    LOG.info("Start bundle for {}", this);
    futures.clear();
    results.clear();
    errors.clear();
    requestCount = 0;
    resultCount = 0;
  }

  @SuppressWarnings("unchecked")
  @ProcessElement
  public void processElement(ProcessContext c, BoundedWindow window) {
    flush(r -> c.output(KV.of(r.input, r.output)));

    final A input = c.element();
    B cached = cacheGet(input);
    if (cached != null) {
      c.output(KV.of(input, cached));
      return;
    }

    final UUID uuid = UUID.randomUUID();
    ListenableFuture<B> future;
    try {
      semaphore.acquire();
      try {
        future = processElement(session.get(instanceId), input);
      } catch (Exception e) {
        semaphore.release();
        LOG.error("Failed to process element", e);
        throw e;
      }
    } catch (InterruptedException e) {
      LOG.error("Failed to acquire semaphore", e);
      throw new RuntimeException("Failed to acquire semaphore", e);
    }
    requestCount++;

    // Handle failure
    Futures.addCallback(future, new FutureCallback<B>() {
      @Override
      public void onSuccess(@Nullable B result) {
        semaphore.release();
      }

      @Override
      public void onFailure(Throwable t) {
        semaphore.release();
        errors.add(t);
        futures.remove(uuid);
      }
    });

    // Handle success
    ListenableFuture<B> f = Futures.transform(future, new Function<B, B>() {
      @Nullable
      @Override
      public B apply(@Nullable B output) {
        cachePut(input, output);
        results.add(new Result(input, output, c.timestamp(), window));
        futures.remove(uuid);
        return output;
      }
    });

    // This `put` may happen after `remove` in the callbacks but it's OK since either the result
    // or the error would've already been pushed to the corresponding queues and we are not losing
    // data. `waitForFutures` in `finishBundle` blocks until all pending futures, including ones
    // that may have already completed, and `startBundle` clears everything.
    futures.put(uuid, f);
  }

  @FinishBundle
  public void finishBundle(FinishBundleContext c) {
    LOG.info("Finish bundle for {}", this);
    if (!futures.isEmpty()) {
      try {
        // Block until all pending futures are complete
        Futures.allAsList(futures.values()).get();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOG.error("Failed to process futures", e);
        throw new RuntimeException("Failed to process futures", e);
      } catch (ExecutionException e) {
        LOG.error("Failed to process futures", e);
        throw new RuntimeException("Failed to process futures", e);
      }
    }
    flush(r -> c.output(KV.of(r.input, r.output), r.timestamp, r.window));

    // Make sure all requests are processed
    Preconditions.checkState(requestCount == resultCount);
  }

  // Flush pending errors and results
  private void flush(Consumer<Result> outputFn) {
    if (!errors.isEmpty()) {
      RuntimeException e = new RuntimeException("Failed to process futures");
      Throwable t = errors.poll();
      while (t != null) {
        e.addSuppressed(t);
        t = errors.poll();
      }
      LOG.error("Failed to process futures", e);
      throw e;
    }
    Result r = results.poll();
    while (r != null) {
      outputFn.accept(r);
      resultCount++;
      r = results.poll();
    }
  }

  private class Result {
    private A input;
    private B output;
    private Instant timestamp;
    private BoundedWindow window;

    Result(A input, B output, Instant timestamp, BoundedWindow window) {
      this.input  = input;
      this.output = output;
      this.timestamp = timestamp;
      this.window = window;
    }
  }

  public interface SerializableSupplier<T> extends Supplier<T>, Serializable {}
}
