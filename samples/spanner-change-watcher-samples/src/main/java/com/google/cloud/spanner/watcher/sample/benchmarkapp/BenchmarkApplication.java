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

import com.google.cloud.spanner.watcher.sample.benchmarkapp.Main.BenchmarkOptions;
import com.google.common.base.Stopwatch;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class BenchmarkApplication {
  private final Updater updater;
  private final Watcher watcher;
  private Stopwatch watch;

  public BenchmarkApplication(BenchmarkOptions options) throws IOException {
    this.updater = new Updater(options);
    this.watcher = new Watcher(options);
  }

  public void run() {
    this.watch = Stopwatch.createStarted();
    System.out.println("Starting updater...");
    this.updater.run();
    System.out.println("Starting watcher...");
    this.watcher.run();
    System.out.println("Benchmark application started");
    System.out.println();
    
    while (true) {
      try {
        printStatus();
        Thread.sleep(100L);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
    }
  }
  
  private void printStatus() {
    StringBuilder builder = new StringBuilder();
    Duration duration = Duration.ofSeconds(watch.elapsed(TimeUnit.SECONDS));
    builder.append(String.format("\r%1$140s\n", String.format("             Run time: %s %s", duration.toString())));
    builder.append(String.format("\r%1$140s\n", String.format("Transactions executed: %d", updater.totalTransactions)));
    builder.append(String.format("\r%1$140s\n", String.format("         Rows updated: %d", updater.totalMutations)));
    builder.append(String.format("\r%1$140s\n", ""));
    builder.append(String.format("\r%1$140s\n", String.format("     Updates received: %d", watcher.receivedChanges)));
    
    System.out.println(builder.toString());
  }
}
