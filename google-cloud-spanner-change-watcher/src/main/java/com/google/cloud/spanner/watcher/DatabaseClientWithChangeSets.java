/*
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.spanner.watcher;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.AsyncRunner;
import com.google.cloud.spanner.AsyncTransactionManager;
import com.google.cloud.spanner.CommitResponse;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Mutation.Op;
import com.google.cloud.spanner.Options.TransactionOption;
import com.google.cloud.spanner.Options.UpdateOption;
import com.google.cloud.spanner.ReadContext;
import com.google.cloud.spanner.ReadOnlyTransaction;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.TimestampBound;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.spanner.TransactionManager;
import com.google.cloud.spanner.TransactionManager.TransactionState;
import com.google.cloud.spanner.TransactionRunner;
import com.google.cloud.spanner.Value;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.UUID;
import java.util.concurrent.Executor;

/**
 * A wrapper around a {@link DatabaseClient} that automatically generates change set id's and adds a
 * mutation for a change set for each read/write transaction. This {@link DatabaseClient} can be
 * used in combination with {@link SpannerTableChangeSetPoller} or {@link
 * SpannerDatabaseChangeSetPoller} to trigger data changed events for tables that do not contain a
 * commit timestamp column.
 */
public class DatabaseClientWithChangeSets implements DatabaseClient {
  /**
   * Interface for generating unique id's for change sets. The default implementation will use a
   * random UUID for each change set.
   */
  public interface ChangeSetIdGenerator {
    String generateChangeSetId();
  }

  /** Supplier of change set id's. */
  public interface ChangeSetIdSupplier {
    String getChangeSetId();
  }

  /** {@link TransactionRunner} that automatically creates a change set. */
  public interface TransactionRunnerWithChangeSet extends TransactionRunner, ChangeSetIdSupplier {}

  /** {@link TransactionManager} that automatically creates a change set. */
  public interface TransactionManagerWithChangeSet
      extends TransactionManager, ChangeSetIdSupplier {}

  /** {@link AsyncRunner} that automatically creates a change set. */
  public interface AsyncRunnerWithChangeSet extends AsyncRunner, ChangeSetIdSupplier {}

  /** {@link AsyncTransactionManager} that automatically creates a change set. */
  public interface AsyncTransactionManagerWithChangeSet
      extends AsyncTransactionManager, ChangeSetIdSupplier {}

  /** Default implementation of {@link ChangeSetIdGenerator} that will use random UUID's. */
  static class RandomUUIDChangeSetIdGenerator implements ChangeSetIdGenerator {
    static final RandomUUIDChangeSetIdGenerator INSTANCE = new RandomUUIDChangeSetIdGenerator();

    private RandomUUIDChangeSetIdGenerator() {}

    @Override
    public String generateChangeSetId() {
      return UUID.randomUUID().toString();
    }
  }

  abstract class AbstractChangeSetTransaction implements ChangeSetIdSupplier {
    final String changeSetId;

    AbstractChangeSetTransaction() {
      this.changeSetId = idGenerator.generateChangeSetId();
    }

    @Override
    public String getChangeSetId() {
      return changeSetId;
    }
  }

  class TransactionRunnerWithChangeSetImpl extends AbstractChangeSetTransaction
      implements TransactionRunnerWithChangeSet {
    final TransactionRunner runner;

    TransactionRunnerWithChangeSetImpl(TransactionRunner runner) {
      this.runner = runner;
    }

    @Override
    public <T> T run(TransactionCallable<T> callable) {
      return runner.run(
          new TransactionCallable<T>() {
            @Override
            public T run(TransactionContext transaction) throws Exception {
              transaction.buffer(createChangeSetMutation(changeSetId));
              return callable.run(transaction);
            }
          });
    }

    @Override
    public Timestamp getCommitTimestamp() {
      return runner.getCommitTimestamp();
    }

    @Override
    public CommitResponse getCommitResponse() {
      return runner.getCommitResponse();
    }

    @Override
    public TransactionRunner allowNestedTransaction() {
      return runner.allowNestedTransaction();
    }
  }

  class TransactionManagerWithChangeSetImpl extends AbstractChangeSetTransaction
      implements TransactionManagerWithChangeSet {
    final TransactionManager manager;

    TransactionManagerWithChangeSetImpl(TransactionManager manager) {
      this.manager = manager;
    }

    @Override
    public TransactionContext begin() {
      TransactionContext context = manager.begin();
      context.buffer(createChangeSetMutation(changeSetId));
      return context;
    }

    @Override
    public void commit() {
      manager.commit();
    }

    @Override
    public void rollback() {
      manager.rollback();
    }

    @Override
    public TransactionContext resetForRetry() {
      TransactionContext context = manager.resetForRetry();
      context.buffer(createChangeSetMutation(changeSetId));
      return context;
    }

    @Override
    public Timestamp getCommitTimestamp() {
      return manager.getCommitTimestamp();
    }

    @Override
    public CommitResponse getCommitResponse() {
      return manager.getCommitResponse();
    }

    @Override
    public TransactionState getState() {
      return manager.getState();
    }

    @Override
    public void close() {
      manager.close();
    }
  }

  class AsyncRunnerWithChangeSetImpl extends AbstractChangeSetTransaction
      implements AsyncRunnerWithChangeSet {
    final AsyncRunner runner;

    AsyncRunnerWithChangeSetImpl(AsyncRunner runner) {
      this.runner = runner;
    }

    @Override
    public <R> ApiFuture<R> runAsync(AsyncWork<R> work, Executor executor) {
      return runner.runAsync(
          new AsyncWork<R>() {
            @Override
            public ApiFuture<R> doWorkAsync(TransactionContext txn) {
              txn.buffer(createChangeSetMutation(changeSetId));
              return work.doWorkAsync(txn);
            }
          },
          executor);
    }

    @Override
    public ApiFuture<Timestamp> getCommitTimestamp() {
      return runner.getCommitTimestamp();
    }

    @Override
    public ApiFuture<CommitResponse> getCommitResponse() {
      return runner.getCommitResponse();
    }
  }

  class AsyncTransactionManagerWithChangeSetImpl extends AbstractChangeSetTransaction
      implements AsyncTransactionManagerWithChangeSet {
    final AsyncTransactionManager manager;

    AsyncTransactionManagerWithChangeSetImpl(AsyncTransactionManager manager) {
      this.manager = manager;
    }

    @Override
    public TransactionContextFuture beginAsync() {
      TransactionContextFuture context = manager.beginAsync();
      ApiFutures.addCallback(
          context,
          new ApiFutureCallback<TransactionContext>() {
            @Override
            public void onFailure(Throwable t) {}

            @Override
            public void onSuccess(TransactionContext context) {
              context.buffer(createChangeSetMutation(changeSetId));
            }
          },
          MoreExecutors.directExecutor());
      return context;
    }

    @Override
    public ApiFuture<Void> rollbackAsync() {
      return manager.rollbackAsync();
    }

    @Override
    public TransactionContextFuture resetForRetryAsync() {
      TransactionContextFuture context = manager.resetForRetryAsync();
      ApiFutures.addCallback(
          context,
          new ApiFutureCallback<TransactionContext>() {
            @Override
            public void onFailure(Throwable t) {}

            @Override
            public void onSuccess(TransactionContext context) {
              context.buffer(createChangeSetMutation(changeSetId));
            }
          },
          MoreExecutors.directExecutor());
      return context;
    }

    @Override
    public TransactionState getState() {
      return manager.getState();
    }

    @Override
    public void close() {
      manager.close();
    }

    @Override
    public ApiFuture<Void> closeAsync() {
      return manager.closeAsync();
    }

    @Override
    public ApiFuture<CommitResponse> getCommitResponse() {
      return manager.getCommitResponse();
    }
  }

  private final DatabaseClient client;
  private final String changeSetTable;
  private final String changeSetIdColumn;
  private final String changeSetCommitTSColumn;
  private final ChangeSetIdGenerator idGenerator;

  /**
   * Creates a {@link DatabaseClient} that will automatically create a change set for each
   * read/write transaction.
   */
  public static DatabaseClientWithChangeSets of(DatabaseClient client) {
    return new DatabaseClientWithChangeSets(
        client,
        SpannerTableChangeSetPoller.DEFAULT_CHANGE_SET_TABLE_NAME,
        SpannerTableChangeSetPoller.DEFAULT_CHANGE_SET_ID_COLUMN,
        SpannerTableChangeSetPoller.DEFAULT_CHANGE_SET_COMMIT_TS_COLUMN,
        RandomUUIDChangeSetIdGenerator.INSTANCE);
  }

  private DatabaseClientWithChangeSets(
      DatabaseClient client,
      String changeSetTable,
      String changeSetIdColumn,
      String changeSetCommitTSColumn,
      ChangeSetIdGenerator idGenerator) {
    this.client = client;
    this.changeSetTable = changeSetTable;
    this.changeSetIdColumn = changeSetIdColumn;
    this.changeSetCommitTSColumn = changeSetCommitTSColumn;
    this.idGenerator = idGenerator;
  }

  public String newChangeSetId() {
    return idGenerator.generateChangeSetId();
  }

  boolean containsChangeSetOperation(Iterable<Mutation> mutations) {
    for (Mutation m : mutations) {
      if (m.getTable().equals(changeSetTable)) {
        if (m.getOperation() == Op.INSERT || m.getOperation() == Op.INSERT_OR_UPDATE) {
          return true;
        }
      }
    }
    return false;
  }

  Mutation createChangeSetMutation(String changeSetId) {
    return Mutation.newInsertOrUpdateBuilder(changeSetTable)
        .set(changeSetIdColumn)
        .to(changeSetId)
        .set(changeSetCommitTSColumn)
        .to(Value.COMMIT_TIMESTAMP)
        .build();
  }

  Iterable<Mutation> appendChangeSetMutation(Iterable<Mutation> mutations, String changeSetId) {
    return Iterables.concat(mutations, ImmutableList.of(createChangeSetMutation(changeSetId)));
  }

  /**
   * Writes the given mutations WITHOUT a change set id. Consider using {@link #write(String,
   * Iterable)}.
   */
  @Override
  public Timestamp write(Iterable<Mutation> mutations) throws SpannerException {
    return client.write(mutations);
  }

  /**
   * Writes the given mutations atomically to the database using the given change set id.
   *
   * <p>This method uses retries and replay protection internally, which means that the mutations
   * are applied exactly once on success, or not at all if an error is returned, regardless of any
   * failures in the underlying network. Note that if the call is cancelled or reaches deadline, it
   * is not possible to know whether the mutations were applied without performing a subsequent
   * database operation, but the mutations will have been applied at most once.
   *
   * <p>Example of blind write.
   *
   * <pre>{@code
   * String changeSetId = dbClient.newChangeSetId();
   * long singerId = my_singer_id;
   * Mutation mutation = Mutation.newInsertBuilder("Singer")
   *         .set("SingerId")
   *         .to(singerId)
   *         .set("FirstName")
   *         .to("Billy")
   *         .set("LastName")
   *         .to("Joel")
   *         .set("ChangeSetId")
   *         .to(changeSetId)
   *         .build();
   * dbClient.write(Collections.singletonList(mutation));
   * }</pre>
   *
   * @return the timestamp at which the write was committed
   */
  public Timestamp write(String changeSetId, Iterable<Mutation> mutations) throws SpannerException {
    return write(appendChangeSetMutation(mutations, changeSetId));
  }

  /**
   * Writes the given mutations WITHOUT a change set id. Consider using {@link
   * #writeAtLeastOnce(String, Iterable)}.
   */
  @Override
  public Timestamp writeAtLeastOnce(Iterable<Mutation> mutations) throws SpannerException {
    return client.writeAtLeastOnce(mutations);
  }

  /**
   * Writes the given mutations atomically to the database without replay protection using the given
   * change set id.
   *
   * <p>Since this method does not feature replay protection, it may attempt to apply {@code
   * mutations} more than once; if the mutations are not idempotent, this may lead to a failure
   * being reported when the mutation was applied once. For example, an insert may fail with {@link
   * ErrorCode#ALREADY_EXISTS} even though the row did not exist before this method was called. For
   * this reason, most users of the library will prefer to use {@link #write(Iterable)} instead.
   * However, {@code writeAtLeastOnce()} requires only a single RPC, whereas {@code write()}
   * requires two RPCs (one of which may be performed in advance), and so this method may be
   * appropriate for latency sensitive and/or high throughput blind writing.
   *
   * <p>Example of unprotected blind write.
   *
   * <pre>{@code
   * String changeSetId = dbClient.newChangeSetId();
   * long singerId = my_singer_id;
   * Mutation mutation = Mutation.newInsertBuilder("Singers")
   *         .set("SingerId")
   *         .to(singerId)
   *         .set("FirstName")
   *         .to("Billy")
   *         .set("LastName")
   *         .to("Joel")
   *         .set("ChangeSetId")
   *         .to(changeSetId)
   *         .build();
   * dbClient.writeAtLeastOnce(changeSetId, Collections.singletonList(mutation));
   * }</pre>
   *
   * @return the timestamp at which the write was committed
   */
  public Timestamp writeAtLeastOnce(String changeSetId, Iterable<Mutation> mutations)
      throws SpannerException {
    return writeAtLeastOnce(appendChangeSetMutation(mutations, changeSetId));
  }

  /**
   * Creates a {@link TransactionRunner} that automatically creates a change set.
   *
   * <p>Example usage:
   *
   * <pre><code>
   * DatabaseClientWithChangeSets dbClientWithChangeSets = DatabaseClientWithChangeSets.of(dbClient);
   * TransactionRunnerWithChangeSet runner = dbClientWithChangeSets.readWriteTransaction();
   * long singerId = my_singer_id;
   * runner.run(
   *     new TransactionCallable&lt;Void&gt;() {
   *
   *       {@literal @}Override
   *       public Void run(TransactionContext transaction) throws Exception {
   *         String column = "FirstName";
   *         Struct row =
   *             transaction.readRow("Singers", Key.of(singerId), Collections.singleton(column));
   *         String name = row.getString(column);
   *         transaction.buffer(
   *             Mutation.newUpdateBuilder("Singers")
   *                 .set(column).to(name.toUpperCase())
   *                 .set("ChangeSetId").to(runner.getChangeSetId())
   *                 .build());
   *         return null;
   *       }
   *     });
   * </code></pre>
   */
  @Override
  public TransactionRunnerWithChangeSet readWriteTransaction(TransactionOption... options) {
    return new TransactionRunnerWithChangeSetImpl(client.readWriteTransaction(options));
  }

  /**
   * Returns a transaction manager that automatically creates a change set. This API is meant for
   * advanced users. Most users should instead use the {@link #readWriteTransaction()} API instead.
   *
   * <p>Example usage:
   *
   * <pre>{@code
   * DatabaseClientWithChangeSets dbClientWithChangeSets = DatabaseClientWithChangeSets.of(dbClient);
   * long singerId = my_singer_id;
   * try (TransactionManagerWithChangeSet manager = dbClientWithChangeSets.transactionManager()) {
   *   TransactionContext txn = manager.begin();
   *   while (true) {
   *     String column = "FirstName";
   *     Struct row = txn.readRow("Singers", Key.of(singerId), Collections.singleton(column));
   *     String name = row.getString(column);
   *     txn.buffer(
   *         Mutation.newUpdateBuilder("Singers")
   *             .set(column).to(name.toUpperCase())
   *             .set("ChangeSetId").to(manager.getChangeSetId())
   *             .build());
   *     try {
   *       manager.commit();
   *       break;
   *     } catch (AbortedException e) {
   *       Thread.sleep(e.getRetryDelayInMillis() / 1000);
   *       txn = manager.resetForRetry();
   *     }
   *   }
   * }
   * }</pre>
   */
  @Override
  public TransactionManagerWithChangeSet transactionManager(TransactionOption... options) {
    return new TransactionManagerWithChangeSetImpl(client.transactionManager(options));
  }

  /**
   * Returns an asynchronous transaction runner that automatically creates a change set for
   * executing a single logical transaction with retries. The returned runner can only be used once.
   *
   * <p>Example usage:
   *
   * <pre> <code>
   * Executor executor = Executors.newSingleThreadExecutor();
   * DatabaseClientWithChangeSets dbClientWithChangeSets = DatabaseClientWithChangeSets.of(dbClient);
   * final long singerId = my_singer_id;
   * AsyncRunnerWithChangeSet runner = dbClientWithChangeSets.runAsync();
   * ApiFuture<Long> rowCount =
   *     runner.runAsync(
   *         new AsyncWork<Long>() {
   *           @Override
   *           public ApiFuture<Long> doWorkAsync(TransactionContext txn) {
   *             String column = "FirstName";
   *             Struct row =
   *                 txn.readRow("Singers", Key.of(singerId), Collections.singleton("Name"));
   *             String name = row.getString("Name");
   *             return txn.executeUpdateAsync(
   *                 Statement.newBuilder("UPDATE Singers SET Name=@name, ChangeSetId=@changeSetId WHERE SingerId=@id")
   *                     .bind("id")
   *                     .to(singerId)
   *                     .bind("name")
   *                     .to(name.toUpperCase())
   *                     .bind("ChangeSetId")
   *                     .to(runner.getChangeSetId())
   *                     .build());
   *           }
   *         },
   *         executor);
   * </code></pre>
   */
  @Override
  public AsyncRunnerWithChangeSet runAsync(TransactionOption... options) {
    return new AsyncRunnerWithChangeSetImpl(client.runAsync(options));
  }

  /**
   * Returns an asynchronous transaction manager that automatically creates a change set, and which
   * allows manual management of transaction lifecycle. This API is meant for advanced users. Most
   * users should instead use the {@link #runAsync()} API instead.
   *
   * <p>Example of using {@link AsyncTransactionManagerWithChangeSet} with lambda expressions (Java
   * 8 and higher).
   *
   * <pre>{@code
   * DatabaseClientWithChangeSets dbClientWithChangeSets = DatabaseClientWithChangeSets.of(dbClient);
   * long singerId = 1L;
   * try (AsyncTransactionManagerWithChangeSet manager = dbClientWithChangeSets.transactionManagerAsync()) {
   *   TransactionContextFuture txnFut = manager.beginAsync();
   *   while (true) {
   *     String column = "FirstName";
   *     CommitTimestampFuture commitTimestamp =
   *         txnFut
   *             .then(
   *                 (txn, __) ->
   *                     txn.readRowAsync(
   *                         "Singers", Key.of(singerId), Collections.singleton(column)))
   *             .then(
   *                 (txn, row) -> {
   *                   String name = row.getString(column);
   *                   txn.buffer(
   *                       Mutation.newUpdateBuilder("Singers")
   *                           .set(column)
   *                           .to(name.toUpperCase())
   *                           .set("ChangeSetId")
   *                           .to(manager.getChangeSetId())
   *                           .build());
   *                   return ApiFutures.immediateFuture(null);
   *                 })
   *             .commitAsync();
   *     try {
   *       commitTimestamp.get();
   *       break;
   *     } catch (AbortedException e) {
   *       Thread.sleep(e.getRetryDelayInMillis() / 1000);
   *       txnFut = manager.resetForRetryAsync();
   *     }
   *   }
   * }
   * }</pre>
   */
  @Override
  public AsyncTransactionManagerWithChangeSet transactionManagerAsync(
      TransactionOption... options) {
    return new AsyncTransactionManagerWithChangeSetImpl(client.transactionManagerAsync(options));
  }

  @Override
  public ReadContext singleUse() {
    return client.singleUse();
  }

  @Override
  public ReadContext singleUse(TimestampBound bound) {
    return client.singleUse(bound);
  }

  @Override
  public ReadOnlyTransaction singleUseReadOnlyTransaction() {
    return client.singleUseReadOnlyTransaction();
  }

  @Override
  public ReadOnlyTransaction singleUseReadOnlyTransaction(TimestampBound bound) {
    return client.singleUseReadOnlyTransaction(bound);
  }

  @Override
  public ReadOnlyTransaction readOnlyTransaction() {
    return client.readOnlyTransaction();
  }

  @Override
  public ReadOnlyTransaction readOnlyTransaction(TimestampBound bound) {
    return client.readOnlyTransaction(bound);
  }

  @Override
  public long executePartitionedUpdate(Statement stmt, UpdateOption... options) {
    return client.executePartitionedUpdate(stmt, options);
  }

  @Override
  public CommitResponse writeWithOptions(Iterable<Mutation> mutations, TransactionOption... options)
      throws SpannerException {
    return client.writeWithOptions(mutations, options);
  }

  @Override
  public CommitResponse writeAtLeastOnceWithOptions(
      Iterable<Mutation> mutations, TransactionOption... options) throws SpannerException {
    return client.writeAtLeastOnceWithOptions(mutations, options);
  }
}
