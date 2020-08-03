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
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Mutation.Op;
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
 * {@link DatabaseClient} that automatically generates change set id's, adds a mutation for a change
 * set for each read/write transaction, and adds the change set id to mutations that are written.
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

    public String generateChangeSetId() {
      return UUID.randomUUID().toString();
    }
  }

  abstract class AbstractChangeSetTransaction implements ChangeSetIdSupplier {
    final String changeSetId;

    AbstractChangeSetTransaction() {
      this.changeSetId = idGenerator.generateChangeSetId();
    }

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

    public Timestamp getCommitTimestamp() {
      return runner.getCommitTimestamp();
    }

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

    public TransactionContext begin() {
      TransactionContext context = manager.begin();
      context.buffer(createChangeSetMutation(changeSetId));
      return context;
    }

    public void commit() {
      manager.commit();
    }

    public void rollback() {
      manager.rollback();
    }

    public TransactionContext resetForRetry() {
      TransactionContext context = manager.resetForRetry();
      context.buffer(createChangeSetMutation(changeSetId));
      return context;
    }

    public Timestamp getCommitTimestamp() {
      return manager.getCommitTimestamp();
    }

    public TransactionState getState() {
      return manager.getState();
    }

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

    public ApiFuture<Timestamp> getCommitTimestamp() {
      return runner.getCommitTimestamp();
    }
  }

  class AsyncTransactionManagerWithChangeSetImpl extends AbstractChangeSetTransaction
      implements AsyncTransactionManagerWithChangeSet {
    final AsyncTransactionManager manager;

    AsyncTransactionManagerWithChangeSetImpl(AsyncTransactionManager manager) {
      this.manager = manager;
    }

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

    public ApiFuture<Void> rollbackAsync() {
      return manager.rollbackAsync();
    }

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

    public TransactionState getState() {
      return manager.getState();
    }

    public void close() {
      manager.close();
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

  @Override
  public Timestamp write(Iterable<Mutation> mutations) throws SpannerException {
    return client.write(mutations);
  }

  /** Writes a set of mutations as a change set using the given change set id. */
  public Timestamp write(String changeSetId, Iterable<Mutation> mutations) throws SpannerException {
    return write(appendChangeSetMutation(mutations, changeSetId));
  }

  @Override
  public Timestamp writeAtLeastOnce(Iterable<Mutation> mutations) throws SpannerException {
    return client.writeAtLeastOnce(mutations);
  }

  /** Writes a set of mutations at least once as a change set using the given change set id. */
  public Timestamp writeAtLeastOnce(String changeSetId, Iterable<Mutation> mutations)
      throws SpannerException {
    return writeAtLeastOnce(appendChangeSetMutation(mutations, changeSetId));
  }

  /** Creates a {@link TransactionRunner} that automatically creates a change set. */
  public TransactionRunnerWithChangeSet readWriteTransaction() {
    return new TransactionRunnerWithChangeSetImpl(client.readWriteTransaction());
  }

  /** Creates a {@link TransactionManager} that automatically creates a change set. */
  public TransactionManagerWithChangeSet transactionManager() {
    return new TransactionManagerWithChangeSetImpl(client.transactionManager());
  }

  public AsyncRunnerWithChangeSet runAsync() {
    return new AsyncRunnerWithChangeSetImpl(client.runAsync());
  }

  public AsyncTransactionManagerWithChangeSet transactionManagerAsync() {
    return new AsyncTransactionManagerWithChangeSetImpl(client.transactionManagerAsync());
  }

  public ReadContext singleUse() {
    return client.singleUse();
  }

  public ReadContext singleUse(TimestampBound bound) {
    return client.singleUse(bound);
  }

  public ReadOnlyTransaction singleUseReadOnlyTransaction() {
    return client.singleUseReadOnlyTransaction();
  }

  public ReadOnlyTransaction singleUseReadOnlyTransaction(TimestampBound bound) {
    return client.singleUseReadOnlyTransaction(bound);
  }

  public ReadOnlyTransaction readOnlyTransaction() {
    return client.readOnlyTransaction();
  }

  public ReadOnlyTransaction readOnlyTransaction(TimestampBound bound) {
    return client.readOnlyTransaction(bound);
  }

  public long executePartitionedUpdate(Statement stmt) {
    return client.executePartitionedUpdate(stmt);
  }
}
