# Google Cloud Spanner Change Watcher and Publisher

*Under construction, please check back later!*

## Introduction
Google Cloud Spanner Change Watcher and Publisher is a framework that emits
events when a row has been changed in a Cloud Spanner database. These row
change events can be captured and processed in-process as part of a user
application, or they can be published to Google Cloud Pubsub for consumption by
any other application.

## Spanner Change Watcher
Spanner Change Watcher is the core framework that watches Spanner databases and
tables for data changes and emits events when changes are detected. This
framework can be included in existing applications and used to trigger
functionality or processes in that application based on data change events.

See [Spanner Change Watcher README file](./google-cloud-spanner-change-watcher/README.md)
for more information.

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


## Spanner Change Archiver
Spanner Change Archiver is an example application using a Spanner Change
Publisher to publish changes in a Google Cloud Spanner database to a Pubsub
topic. This topic is used as a trigger for a Google Cloud Function that writes
the detected change to Google Cloud Storage. This creates a complete change log
for the database in files stored in Google Cloud Storage.

See [Spanner Change Archiver README file](./google-cloud-spanner-change-archiver/README.md)
for more information.

## Support Level
Please feel free to report issues and send pull requests, but note that this
application is not officially supported as part of the Cloud Spanner product.
