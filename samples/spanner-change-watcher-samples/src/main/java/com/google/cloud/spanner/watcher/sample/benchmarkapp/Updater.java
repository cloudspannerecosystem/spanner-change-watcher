/*
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.spanner.watcher.sample.benchmarkapp;

import com.google.api.client.util.Base64;
import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Value;
import com.google.cloud.spanner.watcher.sample.benchmarkapp.Main.BenchmarkOptions;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

public class Updater {
  private final BenchmarkOptions options;
  private final Random rnd = new Random();
  final AtomicLong totalTransactions = new AtomicLong();
  final AtomicLong totalMutations = new AtomicLong();
  private final Spanner spanner;
  private final DatabaseClient client;

  public Updater(BenchmarkOptions options) throws IOException {
    this.options = options;
    spanner = SpannerOptions.newBuilder().build().getService();
    client =
        spanner.getDatabaseClient(
            DatabaseId.of(spanner.getOptions().getProjectId(), options.instance, options.database));
  }

  public void run() {
    final int range = Integer.MAX_VALUE;
    final int threads = options.updateParallelism;
    final int mutationsPerTx = options.mutationsPerTransaction;
    final int tps = options.writeTransactionsPerSecond;
    final int sleepIntervalMs = (1000 / tps) * threads;

    System.out.printf(
        "Starting updater with %d threads executing %d transactions per second with %d mutations per transaction\n",
        threads, tps, mutationsPerTx);

    ExecutorService executor = Executors.newFixedThreadPool(threads);
    for (int i = 0; i < threads; i++) {
      executor.submit(new UpdateRunnable(mutationsPerTx, options.table, range, sleepIntervalMs));
    }
  }

  private class UpdateRunnable implements Runnable {
    private final int numMutationsPerTransaction;
    private final String table;
    private final int range;
    private final int sleepIntervalMs;

    UpdateRunnable(int numMutationsPerTransaction, String table, int range, int sleepIntervalMs) {
      this.numMutationsPerTransaction = numMutationsPerTransaction;
      this.table = table;
      this.range = range;
      this.sleepIntervalMs = sleepIntervalMs;
    }

    @Override
    public void run() {
      final Random rnd = new Random();
      while (true) {
        try {
          Thread.sleep(rnd.nextInt(sleepIntervalMs * 2));
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        }
        int numMutations = rnd.nextInt(numMutationsPerTransaction * 2) + 1;
        List<Mutation> mutations = new ArrayList<Mutation>(numMutations);
        for (int i = 0; i < numMutations; i++) {
          mutations.add(createSingersMutation(rnd, table, range));
        }
        client.write(mutations);
        totalTransactions.incrementAndGet();
        totalMutations.addAndGet(numMutations);
      }
    }
  }

  private Mutation createSingersMutation(Random rnd, String table, int range) {
    return Mutation.newInsertOrUpdateBuilder(table)
        .set("SingerId")
        .to(rnd.nextInt(range))
        .set("FirstName")
        .to(randomString(20))
        .set("LastName")
        .to(randomString(20))
        .set("BirthDate")
        .to(randomDate())
        .set("Picture")
        .to(randomBytes(1024))
        .set("Processed")
        .to(false)
        .set("LastUpdated")
        .to(Value.COMMIT_TIMESTAMP)
        .build();
  }

  private String randomString(int maxLength) {
    byte[] bytes = new byte[rnd.nextInt(maxLength / 2) + 1];
    rnd.nextBytes(bytes);
    return Base64.encodeBase64String(bytes);
  }

  private Date randomDate() {
    return Date.fromYearMonthDay(rnd.nextInt(2019) + 1, rnd.nextInt(11) + 1, rnd.nextInt(28) + 1);
  }

  private ByteArray randomBytes(int maxLength) {
    byte[] bytes = new byte[rnd.nextInt(maxLength) + 1];
    rnd.nextBytes(bytes);
    return ByteArray.copyFrom(bytes);
  }
}
