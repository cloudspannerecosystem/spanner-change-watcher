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

import com.google.cloud.spanner.Options;
import com.google.cloud.spanner.watcher.SpannerTableTailer;
import com.google.cloud.spanner.watcher.sample.benchmarkapp.Main.BenchmarkOptions;
import com.google.cloud.spanner.watcher.sample.benchmarkapp.Watcher.CpuTime;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Runs a simple benchmark application against a single table in a Cloud Spanner database for
 * spanner-change-watcher.
 *
 * <p>The application:
 *
 * <ol>
 *   <li>Starts an {@link Updater} that executes write transactions against the table. The number of
 *       transactions per second (tps), the number of mutations per transaction (m) and the number
 *       of parallel threads to use to execute the transactions can be configured. The total number
 *       of mutations per second is equal to (tps * m). The transactions are distributed evenly (and
 *       randomly) across the threads. The number of parallel threads will determine how many of the
 *       transactions could execute in parallel.
 *   <li>Starts a {@link Watcher} that polls the table for changes using one or more {@link
 *       SpannerTableTailer}s. The configuration of the {@link SpannerTableTailer}s is determined by
 *       the {@link Options} that are passed in to the constructor of this class.
 * </ol>
 */
public class BenchmarkApplication {
  private static final long CYCLE_MS = 550L;
  private static final char ESC_CODE = 0x1B;
  private final BenchmarkOptions options;
  private final Updater updater;
  private final Watcher watcher;
  private Stopwatch watch;
  private long cycle;
  private CpuTime currentCpuTime = new CpuTime();
  private long pollLatency = 0L;

  public BenchmarkApplication(BenchmarkOptions options) throws IOException {
    this.options = options;
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

    if (!options.simpleStatus) {
      System.out.printf(Strings.repeat("\n", 12));
    }
    while (true) {
      try {
        if (options.simpleStatus) {
          printStatusSimple();
        } else {
          printStatusAdvanced();
        }
        Thread.sleep(CYCLE_MS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
    }
  }

  /**
   * Prints a single-line simple status text for this benchmark. This method is used when no (ANSI)
   * console is available.
   */
  private void printStatusSimple() {
    Duration duration = Duration.ofSeconds(watch.elapsed(TimeUnit.SECONDS));
    CpuTime cpuTime = currentCpuTime();
    System.out.printf(
        "\r%s %s: %d tx, %d rows, %d changes, latency: %ds, avg poll: %fs (%d last minute, %d%% CPU)%s",
        cycleSymbol(),
        duration.toString(),
        updater.totalTransactions.longValue(),
        updater.totalMutations.longValue(),
        watcher.receivedChanges.longValue(),
        pollLatency(),
        cpuTime.avg,
        cpuTime.executionCount,
        cpuTime.percentage,
        watcher.isUsingWithQuery() ? " Falling back to WITH query!" : "");
  }

  /**
   * Prints a multi-line status view of this benchmark. This method is used when the application is
   * executed on an ANSI console.
   */
  private void printStatusAdvanced() {
    Duration duration = Duration.ofSeconds(watch.elapsed(TimeUnit.SECONDS));
    CpuTime cpuTime = currentCpuTime();

    System.out.printf("%c[12A", ESC_CODE);
    System.out.printf("------------------------------------------------------------------\n");
    System.out.printf(
        "                                  %c[38;5;118m%s%c[0m             \n",
        ESC_CODE, cycleSymbol(), ESC_CODE);
    System.out.printf(
        "                  Test duration:  %c[38;5;105m%s%c[0m             \n",
        ESC_CODE, duration.toString(), ESC_CODE);
    System.out.printf(
        "                 # Transactions:  %c[38;5;160m%d%c[0m             \n",
        ESC_CODE, updater.totalTransactions.longValue(), ESC_CODE);
    System.out.printf(
        "                    # Mutations:  %c[38;5;196m%d%c[0m             \n",
        ESC_CODE, updater.totalMutations.longValue(), ESC_CODE);
    System.out.printf(
        "             # Received changes:  %c[38;5;122m%d%c[0m             \n",
        ESC_CODE, watcher.receivedChanges.longValue(), ESC_CODE);
    System.out.printf(
        "    Change notification latency:  %c[38;5;128m%d seconds%c[0m     \n",
        ESC_CODE, pollLatency(), ESC_CODE);
    System.out.printf(
        "      Avg poll time last minute:  %c[38;5;129m%f seconds%c[0m     \n",
        ESC_CODE, cpuTime.avg, ESC_CODE);
    System.out.printf(
        "            # Polls last minute:  %c[38;5;118m%d%c[0m             \n",
        ESC_CODE, cpuTime.executionCount, ESC_CODE);
    System.out.printf(
        "   Spanner CPU time last minute:  %c[38;5;119m%s%c[0m             \n",
        ESC_CODE, cpuTime.percentage + "%", ESC_CODE);
    if (watcher.isUsingWithQuery()) {
      System.out.printf("               Falling back to WITH query!                        \n");
    } else {
      System.out.printf("                                                                 \n");
    }
    System.out.printf("------------------------------------------------------------------\n");
  }

  private String cycleSymbol() {
    cycle++;
    switch ((int) (cycle % 3L)) {
      case 0:
        return "/";
      case 1:
        return "-";
      case 2:
        return "\\";
      default:
        return "-";
    }
  }

  private CpuTime currentCpuTime() {
    if (watch.elapsed(TimeUnit.SECONDS) % 11 == 0) {
      CpuTime cpu = watcher.fetchCpuTimeLastMinute();
      if (cpu != null) {
        this.currentCpuTime = cpu;
      }
    }
    return this.currentCpuTime;
  }

  private long pollLatency() {
    if (watch.elapsed(TimeUnit.SECONDS) % 7 == 0) {
      this.pollLatency = watcher.fetchMaxPollLatency();
    }
    return this.pollLatency;
  }
}
