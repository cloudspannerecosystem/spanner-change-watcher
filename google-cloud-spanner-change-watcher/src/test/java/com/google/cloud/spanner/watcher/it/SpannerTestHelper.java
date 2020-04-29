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

import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ServiceOptions;
import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.Instance;
import com.google.cloud.spanner.InstanceConfig;
import com.google.cloud.spanner.InstanceId;
import com.google.cloud.spanner.InstanceInfo;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerOptions;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;

/** Helper class for integration tests using Spanner. */
public final class SpannerTestHelper {
  public static class ITSpannerEnv {
    private Instance instance;
    private String instanceId = System.getProperty("spanner.instance");
    private boolean isOwnedInstance;
    private Spanner spanner;
    private final List<Database> databases = new ArrayList<>();

    public Spanner getSpanner() {
      return spanner;
    }

    public Database createTestDb(Iterable<String> statements)
        throws SpannerException, InterruptedException, ExecutionException {
      String id = String.format(DATABASE_ID_FORMAT, RND.nextInt(100000000));
      Database database =
          spanner.getDatabaseAdminClient().createDatabase(instanceId, id, statements).get();
      databases.add(database);
      return database;
    }
  }

  private static final String INSTANCE_ID_FORMAT = "scw-test-instance-%08d";
  private static final String DATABASE_ID_FORMAT = "scw-test-db-%08d";
  private static final Random RND = new Random();

  private static final String SPANNER_PROJECT_ID =
      System.getProperty("spanner.project", ServiceOptions.getDefaultProjectId());
  private static final String SPANNER_CREDENTIALS_FILE = System.getProperty("spanner.credentials");

  public static void setupSpanner(ITSpannerEnv env) throws Exception {
    env.spanner =
        SpannerOptions.newBuilder()
            .setProjectId(SPANNER_PROJECT_ID)
            .setCredentials(getSpannerCredentials())
            .build()
            .getService();
    if (env.instanceId == null) {
      env.isOwnedInstance = true;
      env.instanceId = String.format(INSTANCE_ID_FORMAT, RND.nextInt(100000000));
    }
    if (env.isOwnedInstance) {
      InstanceConfig instanceConfig =
          env.spanner.getInstanceAdminClient().listInstanceConfigs().getValues().iterator().next();
      env.instance =
          env.spanner
              .getInstanceAdminClient()
              .createInstance(
                  InstanceInfo.newBuilder(InstanceId.of(SPANNER_PROJECT_ID, env.instanceId))
                      .setDisplayName("Test Instance")
                      .setNodeCount(1)
                      .setInstanceConfigId(instanceConfig.getId())
                      .build())
              .get();
    }
  }

  public static void teardownSpanner(ITSpannerEnv env) {
    if (env.isOwnedInstance) {
      env.instance.delete();
    } else {
      for (Database db : env.databases) {
        db.drop();
      }
    }
    env.spanner.close();
  }

  public static String getSpannerProjectId() {
    return SPANNER_PROJECT_ID;
  }

  /**
   * Returns the credentials to use to connect to Cloud Spanner based on the following ordered
   * options:
   *
   * <ol>
   *   <li>The file pointed to by the system variable `spanner.credentials`.
   *   <li>The default credentials of the environment.
   * </ol>
   *
   * The credentials must have permission to create a database on the instance returned by {@link
   * #getInstanceId()}. The credentials must also have permission to create an instance if {@link
   * #getInstanceId()} returns <code>null</code>.
   */
  public static Credentials getSpannerCredentials() throws IOException {
    if (SPANNER_CREDENTIALS_FILE != null) {
      return GoogleCredentials.fromStream(new FileInputStream(SPANNER_CREDENTIALS_FILE));
    }
    return GoogleCredentials.getApplicationDefault();
  }
}
