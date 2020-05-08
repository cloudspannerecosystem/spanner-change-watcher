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

package com.google.cloud.spanner.publisher;

import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ServiceOptions;
import com.google.cloud.spanner.DatabaseId;
import com.google.common.base.MoreObjects;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import org.threeten.bp.Duration;

class Configuration {
  private static final String SCEP_PREFIX = "scep";

  private long maxWaitForShutdownSeconds;

  // Tailer settings.
  private String spannerProject;
  private String spannerInstance;
  private String spannerDatabase;
  private Credentials spannerCredentials;
  private Boolean allTables;
  private List<String> includedTables;
  private List<String> excludedTables;
  private Duration pollInterval;

  // Publisher settings.
  private String pubsubProject;
  private String topicNameFormat;
  private Credentials pubsubCredentials;

  Long getMaxWaitForShutdownSeconds() {
    return maxWaitForShutdownSeconds;
  }

  DatabaseId getDatabaseId() {
    return DatabaseId.of(getSpannerProject(), spannerInstance, spannerDatabase);
  }

  String getSpannerProject() {
    return MoreObjects.firstNonNull(spannerProject, ServiceOptions.getDefaultProjectId());
  }

  Credentials getSpannerCredentials() throws IOException {
    return MoreObjects.firstNonNull(spannerCredentials, GoogleCredentials.getApplicationDefault());
  }

  boolean isAllTables() {
    if (allTables != null) {
      return allTables.booleanValue();
    }
    return includedTables.isEmpty();
  }

  String[] getExcludedTables() {
    return excludedTables.toArray(new String[excludedTables.size()]);
  }

  String[] getIncludedTables() {
    return includedTables.toArray(new String[includedTables.size()]);
  }

  Duration getPollInterval() {
    return pollInterval;
  }

  String getTopicNameFormat() {
    return topicNameFormat;
  }

  String getPubsubProject() {
    return MoreObjects.firstNonNull(pubsubProject, ServiceOptions.getDefaultProjectId());
  }

  Credentials getPubsubCredentials() throws IOException {
    return MoreObjects.firstNonNull(pubsubCredentials, GoogleCredentials.getApplicationDefault());
  }

  static String prefix(String s) {
    return String.format("%s.%s", SCEP_PREFIX, s);
  }

  static String getSystemOrDefaultProperty(String key, Properties def) {
    return getSystemOrDefaultProperty(key, def, false);
  }

  static String getRequiredSystemOrDefaultProperty(String key, Properties def) {
    return getSystemOrDefaultProperty(key, def, true);
  }

  static String getSystemOrDefaultProperty(String key, Properties def, boolean required) {
    String val = System.getProperty(prefix(key), def.getProperty(prefix(key)));
    if (required && Strings.isNullOrEmpty(val)) {
      throw new IllegalArgumentException(
          String.format("Invalid configuration: Property %s is required", prefix(key)));
    }
    return val;
  }

  /** Create configuration from the given default properties and system properties. */
  static Configuration createConfiguration(Properties defaults) throws IOException {
    Configuration config = new Configuration();

    // General settings.
    config.maxWaitForShutdownSeconds =
        Long.valueOf(
            MoreObjects.firstNonNull(
                getSystemOrDefaultProperty("maxWaitForShutdownSeconds", defaults), "10"));

    // Watcher settings.
    config.spannerProject = getSystemOrDefaultProperty("spanner.project", defaults);
    config.spannerInstance = getRequiredSystemOrDefaultProperty("spanner.instance", defaults);
    config.spannerDatabase = getRequiredSystemOrDefaultProperty("spanner.database", defaults);
    String creds = getSystemOrDefaultProperty("spanner.credentials", defaults);
    if (!Strings.isNullOrEmpty(creds)) {
      config.spannerCredentials = GoogleCredentials.fromStream(new FileInputStream(creds));
    }
    config.pollInterval =
        Duration.parse(
            MoreObjects.firstNonNull(
                getSystemOrDefaultProperty("spanner.pollInterval", defaults), "PT0.1S"));

    String allTablesString = getSystemOrDefaultProperty("spanner.allTables", defaults);
    String includedTablesString = getSystemOrDefaultProperty("spanner.includedTables", defaults);
    String excludedTablesString = getSystemOrDefaultProperty("spanner.excludedTables", defaults);
    if (!Strings.isNullOrEmpty(includedTablesString)
        && !Strings.isNullOrEmpty(excludedTablesString)) {
      throw new IllegalArgumentException(
          String.format(
              "Invalid configuration: spanner.includedTables=%s and spanner.excludedTables=%s. Only one of these should be specified.",
              includedTablesString, excludedTablesString));
    }
    if (allTablesString != null) {
      config.allTables = Boolean.valueOf(allTablesString);
      if (config.allTables) {
        if (!Strings.isNullOrEmpty(includedTablesString)) {
          throw new IllegalArgumentException(
              String.format(
                  "Invalid configuration: spanner.allTables=true and spanner.includedTables=%s. The configuration should only include either spanner.allTables=true or a list of included tables, but not both.",
                  includedTablesString));
        }
        config.includedTables = ImmutableList.of();
      } else {
        if (!Strings.isNullOrEmpty(excludedTablesString)) {
          throw new IllegalArgumentException(
              String.format(
                  "Invalid configuration: spanner.allTables=false and spanner.excludedTables=%s. Excluding tables is only allowed in combination with spanner.allTables=true.",
                  excludedTablesString));
        }
        if (Strings.isNullOrEmpty(includedTablesString)) {
          throw new IllegalArgumentException(
              "Invalid configuration: No tables specified. Set either spanner.allTables=true or spanner.includedTables to a non-empty list of table names.");
        }
        config.excludedTables = ImmutableList.of();
      }
    }
    if (Strings.isNullOrEmpty(includedTablesString)) {
      config.includedTables = ImmutableList.of();
    } else {
      config.includedTables = ImmutableList.copyOf(includedTablesString.split(","));
    }
    if (Strings.isNullOrEmpty(excludedTablesString)) {
      config.excludedTables = ImmutableList.of();
    } else {
      config.excludedTables = ImmutableList.copyOf(excludedTablesString.split(","));
    }

    // Publisher settings.
    config.pubsubProject = getSystemOrDefaultProperty("pubsub.project", defaults);
    creds = getSystemOrDefaultProperty("pubsub.credentials", defaults);
    if (!Strings.isNullOrEmpty(creds)) {
      config.pubsubCredentials = GoogleCredentials.fromStream(new FileInputStream(creds));
    }
    config.topicNameFormat = getSystemOrDefaultProperty("pubsub.topicNameFormat", defaults);
    if (Strings.isNullOrEmpty(config.topicNameFormat)) {
      throw new IllegalArgumentException(
          "Invalid configuration: pubsub.topicNameFormat is required.");
    }

    return config;
  }
}
