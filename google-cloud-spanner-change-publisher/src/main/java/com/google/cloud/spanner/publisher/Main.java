package com.google.cloud.spanner.publisher;

import static com.google.cloud.spanner.publisher.Configuration.prefix;

import com.google.api.core.ApiService.Listener;
import com.google.api.core.ApiService.State;
import com.google.api.core.SettableApiFuture;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.watcher.SpannerDatabaseChangeWatcher;
import com.google.cloud.spanner.watcher.SpannerDatabaseTailer;
import com.google.common.util.concurrent.MoreExecutors;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Main class for running a {@link SpannerDatabaseChangeEventPublisher} as a standalone application.
 */
public class Main {
  private static final String DEFAULT_PROPERTIES_FILE_NAME = prefix("properties");
  private static final SettableApiFuture<Void> initialized = SettableApiFuture.create();
  private static SpannerDatabaseChangeEventPublisher publisher;

  public static SpannerDatabaseChangeEventPublisher getPublisher() throws Throwable {
    try {
      initialized.get();
    } catch (ExecutionException e) {
      throw e.getCause();
    }
    return publisher;
  }

  public static void main(String[] args) throws IOException {
    try {
      Configuration config = Configuration.createConfiguration(readPropertiesFromFile());
      Spanner spanner = createSpannerOptions(config).getService();
      SpannerDatabaseChangeWatcher watcher = createWatcher(config, spanner);

      DatabaseId db = config.getDatabaseId();
      DatabaseClient client = spanner.getDatabaseClient(db);
      publisher = createPublisher(config, watcher, client);
      publisher
          .startAsync()
          .addListener(
              new Listener() {
                @Override
                public void failed(State from, Throwable failure) {
                  spanner.close();
                }

                @Override
                public void terminated(State from) {
                  spanner.close();
                }
              },
              MoreExecutors.directExecutor());

      Runtime.getRuntime()
          .addShutdownHook(
              new Thread() {
                @Override
                public void run() {
                  try {
                    publisher
                        .stopAsync()
                        .awaitTerminated(config.getMaxWaitForShutdownSeconds(), TimeUnit.SECONDS);
                  } catch (TimeoutException e) {
                    // ignore.
                  }
                }
              });
      initialized.set(null);
    } catch (Throwable t) {
      initialized.setException(t);
    }
    if (publisher != null) {
      publisher.awaitTerminated();
    }
  }

  static Properties readPropertiesFromFile() throws IOException {
    // If a property file has been specified, it is required to exist.
    // If no property file has been specified, we try to read from the default property file. If
    // that does not exist, we just skip properties from the file and rely on system properties and
    // default values.
    boolean propertyFileSpecified = System.getProperty(prefix("properties")) != null;
    String propertyFile = System.getProperty(prefix("properties"), DEFAULT_PROPERTIES_FILE_NAME);
    File file = new File(propertyFile);
    boolean readProperties = true;
    if (!file.exists()) {
      if (propertyFileSpecified) {
        throw new IOException(
            String.format("The configuration file %s cannot be found", file.getCanonicalPath()));
      } else {
        readProperties = false;
      }
    }
    if (file.isDirectory()) {
      if (propertyFileSpecified) {
        throw new IOException(
            String.format("The configuration file %s is a directory", file.getCanonicalPath()));
      } else {
        readProperties = false;
      }
    }
    Properties properties = new Properties();
    if (readProperties) {
      properties.load(new FileInputStream(propertyFile));
    }
    return properties;
  }

  static SpannerOptions createSpannerOptions(Configuration config) throws IOException {
    return SpannerOptions.newBuilder()
        .setProjectId(config.getSpannerProject())
        .setCredentials(config.getSpannerCredentials())
        .build();
  }

  static SpannerDatabaseChangeWatcher createWatcher(Configuration config, Spanner spanner) {
    SpannerDatabaseTailer.TableSelecter watcherTableSelecter =
        SpannerDatabaseTailer.newBuilder(spanner, config.getDatabaseId());
    SpannerDatabaseTailer.Builder builder;
    if (config.isAllTables()) {
      builder = watcherTableSelecter.allTables().except(config.getExcludedTables());
    } else {
      String[] tables = config.getIncludedTables();
      builder =
          watcherTableSelecter.includeTables(
              tables[0], Arrays.copyOfRange(tables, 1, tables.length));
    }
    if (config.getPollInterval() != null) {
      builder.setPollInterval(config.getPollInterval());
    }
    return builder.build();
  }

  static SpannerDatabaseChangeEventPublisher createPublisher(
      Configuration config, SpannerDatabaseChangeWatcher watcher, DatabaseClient client)
      throws IOException {
    SpannerDatabaseChangeEventPublisher publisher =
        SpannerDatabaseChangeEventPublisher.newBuilder(watcher, client)
            .setTopicNameFormat(
                String.format(
                    "projects/%s/topics/%s",
                    config.getPubsubProject(), config.getTopicNameFormat()))
            .setCredentials(config.getPubsubCredentials())
            .build();
    return publisher;
  }
}
