/*
 * Copyright 2020 Google LLC
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

package com.google.cloud.spanner.watcher.it;

import static com.google.common.truth.Truth.assertThat;

import com.google.api.client.util.Base64;
import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.StructReader;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Value;
import com.google.cloud.spanner.watcher.SpannerCommitTimestampRepository;
import com.google.cloud.spanner.watcher.SpannerTableChangeWatcher.Row;
import com.google.cloud.spanner.watcher.SpannerTableChangeWatcher.RowChangeCallback;
import com.google.cloud.spanner.watcher.SpannerTableTailer;
import com.google.cloud.spanner.watcher.TableId;
import com.google.cloud.spanner.watcher.TimebasedShardProvider;
import com.google.cloud.spanner.watcher.TimebasedShardProvider.Interval;
import com.google.cloud.spanner.watcher.TimebasedShardProvider.TimebasedShardId;
import com.google.cloud.spanner.watcher.it.SpannerTestHelper.ITSpannerEnv;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.threeten.bp.Duration;

@RunWith(Parameterized.class)
public class ITSpannerTableTailerStressTest {
  static final String TABLE_NAME = "TEST_TABLE";
  static final String CREATE_TABLE =
      "CREATE TABLE %s (\n"
          + "  ColInt64       INT64       NOT NULL,\n"
          + "  ColFloat64     FLOAT64     NOT NULL,\n"
          + "  ColBool        BOOL        NOT NULL,\n"
          + "  ColString      STRING(100) NOT NULL,\n"
          + "  ColStringMax   STRING(MAX) NOT NULL,\n"
          + "  ColBytes       BYTES(100)  NOT NULL,\n"
          + "  ColBytesMax    BYTES(MAX)  NOT NULL,\n"
          + "  ColDate        DATE        NOT NULL,\n"
          + "  ColTimestamp   TIMESTAMP   NOT NULL,\n"
          + "  ColShardId     STRING(MAX)         ,\n"
          + "  ShardInt64     INT64               ,\n"
          + "  ShardFloat64   FLOAT64             ,\n"
          + "  ShardBool      BOOL                ,\n"
          + "  ShardString    STRING(100)         ,\n"
          + "  ShardBytes     BYTES(100)          ,\n"
          + "  ShardDate      DATE                ,\n"
          + "  ShardTimestamp TIMESTAMP           ,\n"
          + "  ColCommitTS    TIMESTAMP   NOT NULL OPTIONS (allow_commit_timestamp=true),\n"
          + "  \n"
          + "  ColInt64Array     ARRAY<INT64>,\n"
          + "  ColFloat64Array   ARRAY<FLOAT64>,\n"
          + "  ColBoolArray      ARRAY<BOOL>,\n"
          + "  ColStringArray    ARRAY<STRING(100)>,\n"
          + "  ColStringMaxArray ARRAY<STRING(MAX)>,\n"
          + "  ColBytesArray     ARRAY<BYTES(100)>,\n"
          + "  ColBytesMaxArray  ARRAY<BYTES(MAX)>,\n"
          + "  ColDateArray      ARRAY<DATE>,\n"
          + "  ColTimestampArray ARRAY<TIMESTAMP>\n"
          + ") PRIMARY KEY (ColInt64)\n";
  static final String CREATE_SHARD_INDEX = "CREATE INDEX IDX_%s_SHARD ON %s (ColShardId)";
  private static final Random rnd = new Random();

  @Parameter(0)
  public int changeCount;

  @Parameter(1)
  public int changeRunners;

  @Parameters(name = "change count= {0}, runners= {1}")
  public static Collection<Object[]> parameters() {
    List<Object[]> params = new ArrayList<>();
    for (int runners = 8; runners <= 256; runners *= 2) {
      for (int changeCount = runners; changeCount <= 1024; changeCount *= 2) {
        params.add(new Object[] {changeCount, runners});
      }
    }
    return params;
  }

  private static final Logger logger =
      Logger.getLogger(ITSpannerTableTailerStressTest.class.getName());
  private static final ITSpannerEnv env = new ITSpannerEnv();
  private static Database database;
  private final Object lock = new Object();
  private final ConcurrentMap<Long, Timestamp> lastWrittenTimestamps =
      new ConcurrentHashMap<Long, Timestamp>();
  private final ConcurrentMap<Long, Timestamp> lastReceivedTimestamps =
      new ConcurrentHashMap<Long, Timestamp>();
  private final AtomicInteger sentChanges = new AtomicInteger();

  @BeforeClass
  public static void setup() throws Exception {
    SpannerTestHelper.setupSpanner(env);
    database =
        env.createTestDb(
            ImmutableList.of(
                String.format(CREATE_TABLE, TABLE_NAME),
                String.format(CREATE_SHARD_INDEX, TABLE_NAME, TABLE_NAME)));
    logger.info(String.format("Created database %s", database.getId().toString()));
  }

  @AfterClass
  public static void teardown() {
    SpannerTestHelper.teardownSpanner(env);
  }

  @After
  public void deleteTestData() {
    Spanner spanner = env.getSpanner();
    DatabaseClient client = spanner.getDatabaseClient(database.getId());
    client.write(Collections.singleton(Mutation.delete(TABLE_NAME, KeySet.all())));
    sentChanges.set(0);
  }

  @Test
  public void testStressSpannerTailer() throws Exception {
    System.out.printf("Starting test (changeCount=%d, runners=%d)\n", changeCount, changeRunners);
    Spanner spanner = env.getSpanner();
    SpannerTableTailer watcher =
        SpannerTableTailer.newBuilder(spanner, TableId.of(database.getId(), TABLE_NAME))
            .setPollInterval(Duration.ofMillis(1L))
            .setCommitTimestampRepository(
                SpannerCommitTimestampRepository.newBuilder(spanner, database.getId())
                    .setInitialCommitTimestamp(Timestamp.MIN_VALUE)
                    .build())
            // Use timebased automatic sharding of the table.
            .setShardProvider(TimebasedShardProvider.create("ColShardId", Interval.MINUTE_OF_HOUR))
            .build();
    final CountDownLatch latch = new CountDownLatch(1);
    watcher.addCallback(
        new RowChangeCallback() {
          @Override
          public void rowChange(TableId table, Row row, Timestamp commitTimestamp) {
            lastReceivedTimestamps.put(row.getLong("ColInt64"), commitTimestamp);
            if (sentChanges.get() == changeCount
                && lastReceivedTimestamps.equals(lastWrittenTimestamps)) {
              latch.countDown();
            }
          }
        });
    watcher.startAsync().awaitRunning();
    System.out.printf(
        "Change watcher started (changeCount=%d, runners=%d)\n", changeCount, changeRunners);

    Stopwatch watch = Stopwatch.createStarted();
    DatabaseClient client = spanner.getDatabaseClient(database.getId());
    ListeningExecutorService executor =
        MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(changeRunners));
    List<ListenableFuture<Void>> futures = new ArrayList<>(changeRunners);
    for (int i = 0; i < changeRunners; i++) {
      futures.add(
          executor.submit(new GenerateChangesCallable(client, changeCount / changeRunners)));
    }
    Futures.allAsList(futures).get(300L, TimeUnit.SECONDS);
    System.out.printf(
        "Finished writing changes in %d seconds (changeCount=%d, runners=%d)\n",
        watch.elapsed(TimeUnit.SECONDS), changeCount, changeRunners);
    latch.await(300L, TimeUnit.SECONDS);
    System.out.printf(
        "Finished test in %d seconds (changeCount=%d, runners=%d)\n",
        watch.elapsed(TimeUnit.SECONDS), changeCount, changeRunners);
    assertThat(lastReceivedTimestamps).isEqualTo(lastWrittenTimestamps);
    watcher.stopAsync().awaitTerminated();
    executor.shutdown();
  }

  final class GenerateChangesCallable implements Callable<Void> {
    private final DatabaseClient client;
    private final int numChanges;
    private final Interval shardInterval = Interval.MINUTE_OF_HOUR;
    private TimebasedShardId currentShardId;

    GenerateChangesCallable(DatabaseClient client, int numChanges) {
      this.client = client;
      this.numChanges = numChanges;
    }

    @Override
    public Void call() {
      for (int i = 0; i < numChanges; i++) {
        // Check if we need to refresh the shard value.
        if (currentShardId == null || currentShardId.shouldRefresh()) {
          currentShardId = shardInterval.getCurrentShardId(client.singleUse());
        }
        Mutation mutation =
            createRandomMutation(TABLE_NAME, "ColShardId", currentShardId.getValue(), changeCount);
        Timestamp ts = client.write(Collections.singleton(mutation));
        sentChanges.incrementAndGet();
        Long key = mutation.asMap().get("ColInt64").getInt64();
        synchronized (lock) {
          Timestamp current = lastWrittenTimestamps.get(key);
          if (current == null || ts.compareTo(current) > 0) {
            lastWrittenTimestamps.put(key, ts);
          }
        }
      }
      return null;
    }
  }

  static Mutation createRandomMutation(
      String table, String shardColumn, Value shardValue, int changeCount) {
    return createRandomMutation(table, rnd.nextInt(changeCount / 2), shardColumn, shardValue);
  }

  static Mutation createRandomMutation(
      String table, long id, String shardColumn, Value shardValue) {
    return Mutation.newInsertOrUpdateBuilder(table)
        .set("ColInt64")
        .to(id)
        .set("ColFloat64")
        .to(rnd.nextDouble())
        .set("ColBool")
        .to(rnd.nextBoolean())
        .set("ColString")
        .to(randomString(100))
        .set("ColStringMax")
        .to(randomString(1000))
        .set("ColBytes")
        .to(randomBytes(100))
        .set("ColBytesMax")
        .to(randomBytes(1000))
        .set("ColDate")
        .to(randomDate())
        .set("ColTimestamp")
        .to(randomTimestamp())
        .set(shardColumn)
        .to(shardValue)
        .set("ColCommitTS")
        .to(Value.COMMIT_TIMESTAMP)
        .set("ColInt64Array")
        .toInt64Array(randomLongs(1000))
        .set("ColFloat64Array")
        .toFloat64Array(randomDoubles(1000))
        .set("ColBoolArray")
        .toBoolArray(randomBooleans(1000))
        .set("ColStringArray")
        .toStringArray(randomStrings(100))
        .set("ColStringMaxArray")
        .toStringArray(randomStrings(100))
        .set("ColBytesArray")
        .toBytesArray(randomBytesArray(100))
        .set("ColBytesMaxArray")
        .toBytesArray(randomBytesArray(100))
        .set("ColDateArray")
        .toDateArray(randomDates(100))
        .set("ColTimestampArray")
        .toTimestampArray(randomTimestamps(100))
        .build();
  }

  static String randomString(int maxLength) {
    final int length = rnd.nextInt(maxLength / 2) + 1;
    final byte[] stringBytes = new byte[length];
    rnd.nextBytes(stringBytes);
    return Base64.encodeBase64String(stringBytes);
  }

  static ByteArray randomBytes(int maxLength) {
    final int length = rnd.nextInt(maxLength) + 1;
    final byte[] bytes = new byte[length];
    rnd.nextBytes(bytes);
    return ByteArray.copyFrom(bytes);
  }

  static Date randomDate() {
    return Date.fromYearMonthDay(rnd.nextInt(2020) + 1, rnd.nextInt(11) + 1, rnd.nextInt(28) + 1);
  }

  static Timestamp randomTimestamp() {
    return Timestamp.ofTimeMicroseconds(rnd.nextInt(100_000_000) + 1);
  }

  static long[] randomLongs(int maxLength) {
    final int length = rnd.nextInt(maxLength) + 1;
    return rnd.longs(length).toArray();
  }

  static double[] randomDoubles(int maxLength) {
    final int length = rnd.nextInt(maxLength) + 1;
    return rnd.doubles(length).toArray();
  }

  static Iterable<Boolean> randomBooleans(int maxLength) {
    final int length = rnd.nextInt(maxLength) + 1;
    return IntStream.range(0, length).mapToObj(i -> rnd.nextBoolean()).collect(Collectors.toList());
  }

  static Iterable<String> randomStrings(int maxLength) {
    final int length = rnd.nextInt(maxLength) + 1;
    return IntStream.range(0, length).mapToObj(i -> randomString(100)).collect(Collectors.toList());
  }

  static Iterable<ByteArray> randomBytesArray(int maxLength) {
    final int length = rnd.nextInt(maxLength) + 1;
    return IntStream.range(0, length).mapToObj(i -> randomBytes(100)).collect(Collectors.toList());
  }

  static Iterable<Date> randomDates(int maxLength) {
    final int length = rnd.nextInt(maxLength) + 1;
    return IntStream.range(0, length).mapToObj(i -> randomDate()).collect(Collectors.toList());
  }

  static Iterable<Timestamp> randomTimestamps(int maxLength) {
    final int length = rnd.nextInt(maxLength) + 1;
    return IntStream.range(0, length).mapToObj(i -> randomTimestamp()).collect(Collectors.toList());
  }

  static final class MapStructReader implements StructReader {
    private final Map<String, Value> values;

    MapStructReader(Map<String, Value> values) {
      this.values = values;
    }

    @Override
    public Type getType() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getColumnCount() {
      return values.size();
    }

    @Override
    public int getColumnIndex(String columnName) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Type getColumnType(int columnIndex) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Type getColumnType(String columnName) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isNull(int columnIndex) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isNull(String columnName) {
      return values.get(columnName).isNull();
    }

    @Override
    public boolean getBoolean(int columnIndex) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean getBoolean(String columnName) {
      return values.get(columnName).getBool();
    }

    @Override
    public long getLong(int columnIndex) {
      throw new UnsupportedOperationException();
    }

    @Override
    public long getLong(String columnName) {
      return values.get(columnName).getInt64();
    }

    @Override
    public double getDouble(int columnIndex) {
      throw new UnsupportedOperationException();
    }

    @Override
    public double getDouble(String columnName) {
      return values.get(columnName).getFloat64();
    }

    @Override
    public String getString(int columnIndex) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String getString(String columnName) {
      return values.get(columnName).getString();
    }

    @Override
    public ByteArray getBytes(int columnIndex) {
      throw new UnsupportedOperationException();
    }

    @Override
    public ByteArray getBytes(String columnName) {
      return values.get(columnName).getBytes();
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Timestamp getTimestamp(String columnName) {
      return values.get(columnName).getTimestamp();
    }

    @Override
    public Date getDate(int columnIndex) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Date getDate(String columnName) {
      return values.get(columnName).getDate();
    }

    @Override
    public boolean[] getBooleanArray(int columnIndex) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean[] getBooleanArray(String columnName) {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<Boolean> getBooleanList(int columnIndex) {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<Boolean> getBooleanList(String columnName) {
      return values.get(columnName).getBoolArray();
    }

    @Override
    public long[] getLongArray(int columnIndex) {
      throw new UnsupportedOperationException();
    }

    @Override
    public long[] getLongArray(String columnName) {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<Long> getLongList(int columnIndex) {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<Long> getLongList(String columnName) {
      return values.get(columnName).getInt64Array();
    }

    @Override
    public double[] getDoubleArray(int columnIndex) {
      throw new UnsupportedOperationException();
    }

    @Override
    public double[] getDoubleArray(String columnName) {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<Double> getDoubleList(int columnIndex) {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<Double> getDoubleList(String columnName) {
      return values.get(columnName).getFloat64Array();
    }

    @Override
    public List<String> getStringList(int columnIndex) {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<String> getStringList(String columnName) {
      return values.get(columnName).getStringArray();
    }

    @Override
    public List<ByteArray> getBytesList(int columnIndex) {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<ByteArray> getBytesList(String columnName) {
      return values.get(columnName).getBytesArray();
    }

    @Override
    public List<Timestamp> getTimestampList(int columnIndex) {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<Timestamp> getTimestampList(String columnName) {
      return values.get(columnName).getTimestampArray();
    }

    @Override
    public List<Date> getDateList(int columnIndex) {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<Date> getDateList(String columnName) {
      return values.get(columnName).getDateArray();
    }

    @Override
    public List<Struct> getStructList(int columnIndex) {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<Struct> getStructList(String columnName) {
      throw new UnsupportedOperationException();
    }
  }
}
