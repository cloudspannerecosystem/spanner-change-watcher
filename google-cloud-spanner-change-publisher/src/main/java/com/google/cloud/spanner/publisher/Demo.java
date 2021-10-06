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

package com.google.cloud.spanner.publisher;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.Value;
import com.google.cloud.spanner.watcher.SpannerEmulatorUtil;
import com.google.cloud.spanner.watcher.TableId;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.concurrent.ExecutionException;

public class Demo {

  static void createSpannerDemoDatabase(Configuration config, Spanner spanner) {
    Preconditions.checkState(Configuration.isSpannerEmulator(), "SPANNER_EMULATOR_HOST is not set. Demo mode is only allowed if both SPANNER_EMULATOR_HOST and PUBSUB_EMULATOR_HOST has been set.");
    Preconditions.checkState(Configuration.isPubsubEmulator(), "PUBSUB_EMULATOR_HOST is not set. Demo mode is only allowed if both SPANNER_EMULATOR_HOST and PUBSUB_EMULATOR_HOST has been set.");
    SpannerEmulatorUtil.maybeCreateInstanceAndDatabase(spanner, config.getDatabaseId());
    try {
      spanner.getDatabaseAdminClient().updateDatabaseDdl(config.getDatabaseId().getInstanceId().getInstance(), config.getDatabaseId().getDatabase(),
          ImmutableList.of(
              "CREATE TABLE Singers ("
                  + "SingerId INT64 NOT NULL, "
                  + "FirstName STRING(100), "
                  + "LastName STRING(200) NOT NULL, "
                  + "FullName STRING(300) AS (COALESCE(FirstName || ' ', '') || LastName) STORED, "
                  + "LastUpdated TIMESTAMP OPTIONS (allow_commit_timestamp=true), "
                  + ") PRIMARY KEY (SingerId)",
              "CREATE TABLE Albums ("
                  + "SingerId INT64 NOT NULL, "
                  + "AlbumId INT64 NOT NULL, "
                  + "Title STRING(100) NOT NULL, "
                  + "LastUpdated TIMESTAMP OPTIONS (allow_commit_timestamp=true), "
                  + ") PRIMARY KEY (SingerId, AlbumId), INTERLEAVE IN PARENT Singers"
          ), null).get();
    } catch (ExecutionException e) {
      SpannerException spannerException = SpannerExceptionFactory.asSpannerException(e.getCause());
      // Ignore if the error just indicates that the tables already exist.
      if (!(spannerException.getErrorCode() == ErrorCode.FAILED_PRECONDITION && spannerException.getMessage().contains("Duplicate name in schema"))) {
        throw spannerException;
      }
    } catch (InterruptedException e) {
      throw SpannerExceptionFactory.propagateInterrupt(e);
    }
  }

  static void createDemoSubscriptions(Configuration config) {
    Preconditions.checkState(Configuration.isSpannerEmulator(), "SPANNER_EMULATOR_HOST is not set. Demo mode is only allowed if both SPANNER_EMULATOR_HOST and PUBSUB_EMULATOR_HOST has been set.");
    Preconditions.checkState(Configuration.isPubsubEmulator(), "PUBSUB_EMULATOR_HOST is not set. Demo mode is only allowed if both SPANNER_EMULATOR_HOST and PUBSUB_EMULATOR_HOST has been set.");
    for (String tableName : new String[] {"Singers", "Albums"}) {
      TableId table = TableId.of(config.getDatabaseId(), tableName);
      String topicName =
          config.getTopicNameFormat()
              .replace("%project%", table.getDatabaseId().getInstanceId().getProject())
              .replace(
                  "%instance%", table.getDatabaseId().getInstanceId().getInstance())
              .replace("%database%", table.getDatabaseId().getDatabase())
              .replace("%catalog%", table.getCatalog())
              .replace("%schema%", table.getSchema())
              .replace("%table%", table.getTable());
      String subscriptionName = String.format("spanner-update-%s-%s", config.getDatabaseId().getDatabase(), tableName);
      PubsubEmulatorUtil.maybeCreateSubscriptions(config.getPubsubProject(), topicName, subscriptionName);
    }
  }

  static void executeDemoMutations(Configuration config, Spanner spanner) {
    DatabaseClient client = spanner.getDatabaseClient(config.getDatabaseId());
    client.write(
        ImmutableList.of(
            Mutation.newInsertOrUpdateBuilder("Singers")
                .set("SingerId")
                .to(1L)
                .set("FirstName")
                .to("Robin")
                .set("LastName")
                .to("Allison")
                .set("LastUpdated")
                .to(Value.COMMIT_TIMESTAMP)
                .build(),
        Mutation.newInsertOrUpdateBuilder("Singers")
            .set("SingerId")
            .to(2L)
            .set("FirstName")
            .to("Alice")
            .set("LastName")
            .to("Robinson")
            .set("LastUpdated")
            .to(Value.COMMIT_TIMESTAMP)
            .build()
        ));
  }

}
