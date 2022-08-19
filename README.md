# Google Cloud Spanner Change Watcher and Publisher

__Cloud Spanner has officially released native change streams support, which is recommended instead of this solution.__

For more information see https://cloud.google.com/spanner/docs/change-streams.

## Introduction
Google Cloud Spanner Change Watcher and Publisher is a framework that emits
events when a row has been changed in a Cloud Spanner database. These row
change events can be captured and processed in-process as part of a user
application, or they can be published to Google Cloud Pubsub for consumption by
any other application.

More information on how to use Spanner Change Watcher and Spanner Change Publisher
can be found here:
* [Spanner Change Watcher](https://medium.com/@knutolavloite/cloud-spanner-change-watcher-b77ca036459c) 
* [Spanner Change Publisher](https://medium.com/@knutolavloite/cloud-spanner-change-publisher-7fbee48f66f8)

## Spanner Change Watcher
Spanner Change Watcher is the core framework that watches Spanner databases and
tables for data changes and emits events when changes are detected. This
framework can be included in existing applications and used to trigger
functionality or processes in that application based on data change events.

Spanner Change Watcher by default uses [commit timestamps](https://cloud.google.com/spanner/docs/commit-timestamp)
to determine when a change has occurred.

The framework can also be used for tables that do not include a commit timestamp, but this does require
the use of an additional CHANGE_SETS table that registers all read/write transactions.

See [Spanner Change Watcher README file](./google-cloud-spanner-change-watcher/README.md)
for more information.

### Performance
* See [this article](https://medium.com/@knutolavloite/scaling-up-spanner-change-watcher-82315fbc8962) for more information on how to scale Spanner Change Watcher for large tables using a secondary index that contains a (computed) shard column and the commit timestamp column.
* See [this article](https://medium.com/@knutolavloite/benchmark-spanner-change-watcher-e5b6cc2ac618) for more information on how to benchmark different configurations of Spanner Change Watcher using the built-in Benchmark Application in the samples directory.

### Example Usage

```java
String instance = "my-instance";
String database = "my-database";

Spanner spanner = SpannerOptions.getDefaultInstance().getService();
DatabaseId databaseId = DatabaseId.of(SpannerOptions.getDefaultProjectId(), instance, database);
SpannerDatabaseChangeWatcher watcher =
    SpannerDatabaseTailer.newBuilder(spanner, databaseId).allTables().build();
watcher.addCallback(
    new RowChangeCallback() {
      @Override
      public void rowChange(TableId table, Row row, Timestamp commitTimestamp) {
        System.out.printf(
            "Received change for table %s: %s%n", table, row.asStruct().toString());
      }
    });
watcher.startAsync().awaitRunning();
```

Take a look at [Samples.java](./samples/spanner-change-watcher-samples/src/main/java/com/google/cloud/spanner/watcher/sample/Samples.java)
for additional examples of more advanced use cases.

## Spanner Change Publisher
Spanner Change Publisher publishes data changes in a Cloud Spanner database to
Google Cloud Pubsub. The change events can be consumed by any other application
subscribing to the topic(s) used by the Spanner Change Publisher.

Spanner Change Publisher can be executed as a stand-alone application or it can
be included as a dependency in an existing application.

See [Spanner Change Publisher README file](./google-cloud-spanner-change-publisher/README.md)
for more information.

### Example Usage

```java
String instance = "my-instance";
String database = "my-database";
// The %table% placeholder will automatically be replaced with the name of the table where the
// change occurred.
String topicNameFormat =
    String.format(
        "projects/%s/topics/spanner-update-%%table%%", ServiceOptions.getDefaultProjectId());

// Setup Spanner change watcher.
Spanner spanner = SpannerOptions.newBuilder().build().getService();
DatabaseId databaseId = DatabaseId.of(SpannerOptions.getDefaultProjectId(), instance, database);
SpannerDatabaseChangeWatcher watcher =
    SpannerDatabaseTailer.newBuilder(spanner, databaseId).allTables().build();

// Setup Spanner change publisher.
DatabaseClient client = spanner.getDatabaseClient(databaseId);
SpannerDatabaseChangeEventPublisher eventPublisher =
    SpannerDatabaseChangeEventPublisher.newBuilder(watcher, client)
        .setTopicNameFormat(topicNameFormat)
        .build();
// Start the change publisher. This will automatically also start the change watcher.
eventPublisher.startAsync().awaitRunning();
```

Take a look at [Samples.java](./samples/spanner-change-publisher-samples/src/main/java/com/google/cloud/spanner/publisher/sample/Samples.java)
for additional examples of more advanced use cases.

## Spanner Change Archiver
Spanner Change Archiver is an example application using a Spanner Change
Publisher to publish changes in a Google Cloud Spanner database to a Pubsub
topic. This topic is used as a trigger for a Google Cloud Function that writes
the detected change to Google Cloud Storage. This creates a complete change log
for the database in files stored in Google Cloud Storage.

See [Spanner Change Archiver README file](./google-cloud-spanner-change-archiver/README.md)
for more information.

## Limitations
* Cloud Spanner has officially released native change streams support, which is recommended instead of this solution.
  For more information see https://cloud.google.com/spanner/docs/change-streams.
* Spanner Change Watcher does not support PostgreSQL dialect databases.
* Spanner Change Watcher and Spanner Change Publisher by default use
  [commit timestamps](https://cloud.google.com/spanner/docs/commit-timestamp) to determine when a
  change has occurred. Tables that do not include a commit timestamp can also be monitored, but require
  the creation of an additional CHANGE_SETS table that registers all read/write transactions.
* Deletes are not detected, unless these are soft deletes that only update a deleted flag in the corresponding table.
* Spanner Change Watcher polls tables for changes. Polling on larger tables can take some time and cause some delay
  before a change is detected. The default poll interval is 1 second and is configurable. If multiple changes happen
  during a single poll interval, only the last change will be detected.
* Spanner Change Watcher emits changes on a row level basis, including the commit timestamp of the change. It does not
  emit an event containing all changes of a single transaction. If that is needed, the client application will need to
  group the row level changes together based on the commit timestamp.
* Spanner Change Watcher is not a managed solution and does not come with Cloud Spanner's SLO.

## Support Level
Please feel free to report issues and send pull requests, but note that this
application is not officially supported as part of the Cloud Spanner product.
