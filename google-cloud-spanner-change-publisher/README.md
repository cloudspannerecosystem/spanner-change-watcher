# Spanner Change Publisher

## Introduction
Spanner Change Publisher watches a Spanner database for changes and publishes
these changes to Pubsub. It can monitor a single table, a set of tables, or all
tables in a database.

Spanner Change Publisher can be included in an existing application, or it can
be deployed as a standalone application.

## Usage in Existing Application
Include the following dependency to your application to include the Spanner
Change Publisher.

```xml
<dependency>
  <groupId>com.google.cloud</groupId>
  <artifactId>google-cloud-spanner-change-publisher</artifactId>
  <version>0.0.1-SNAPSHOT</version>
</dependency>
```

### Publish Changes from a Single Table
Publishes change events from a single table in a Spanner database to a Pubsub
topic. The changed record is included in the Pubsub message as an Avro record.

```java
String instance = "my-instance";
String database = "my-database";
String table = "MY_TABLE";
String topicName =
    String.format("projects/%s/topics/my-topic", ServiceOptions.getDefaultProjectId());

// Setup a Spanner change watcher.
Spanner spanner = SpannerOptions.getDefaultInstance().getService();
DatabaseId databaseId = DatabaseId.of(SpannerOptions.getDefaultProjectId(), instance, database);
TableId tableId = TableId.of(databaseId, table);
SpannerTableChangeWatcher watcher = SpannerTableTailer.newBuilder(spanner, tableId).build();

// Setup Spanner change publisher.
DatabaseClient client = spanner.getDatabaseClient(databaseId);
SpannerTableChangeEventPublisher eventPublisher =
    SpannerTableChangeEventPublisher.newBuilder(watcher, client)
        .setTopicName(topicName)
        .build();
// Start the change publisher. This will automatically also start the change watcher.
eventPublisher.startAsync().awaitRunning();
```

### Publish Changes from All Tables
Publishes change events from all tables in a Spanner database to a separate
Pubsub topic per table. The changed record is included in the Pubsub message as
an Avro record.

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

## Use as a Standalone Application
Spanner Change Event Publisher can be run as a standalone application to
publish changes from a Spanner database to Pubsub. Follow these steps to build
and start the application:

1. Build the application including all dependencies by executing `mvn package` in the root of this project. This will generate the file `spanner-publisher.jar` in the target folder.
1. Configure the required properties to specify the Spanner database to monitor and the Pubsub topic to publish to. The configuration can be specified using system properties or a properties file. The below example uses the minimum set of system properties that is needed to monitor all tables in a database and publish the changes to a separate Pubsub topic per table.
1. Start the application using the command `java -Dscep.spanner.instance=my-instance -Dscep.spanner.database=my-database -Dscep.pubsub.topicNameFormat=spanner-update-%table% -jar target/spanner-publisher.jar`.

### Configuration
Additional configuration can be specified using system properties or a
properties file. The application will by default look for a file named
`scep.properties` in the current directory, but a different file and/or
location can be specified using the system property `scep.properties`.

Example:
`java -Dscep.properties=/path/to/configuration.properties spanner-publisher.jar`

The most important configuration parameters are:

```
# Spanner database to monitor
scep.spanner.project=my-project
scep.spanner.instance=my-instance
scep.spanner.database=my-database

# Specific credentials to use for Spanner
scep.spanner.credentials=/path/to/credentials.json

# Selection of which tables to monitor

# Monitor all tables in the database. Cannot be used in combination with
# scep.spanner.includedTables
scep.spanner.allTables=true

# Exclude these tables. Can only be used in combination with
# scep.spanner.allTables=true
scep.spanner.excludedTables=TABLE1,TABLE2,TABLE3

# Include these tables. Can only be used in combination with
# scep.spanner.allTables=false
scep.spanner.includedTables=TABLE1,TABLE2,TABLE3


# Pubsub project and topic name (format)
scep.pubsub.project=my-pubsub-project
scep.pubsub.topicNameFormat=spanner-update-%database%-%table%

# Specific credentials to use for Pubsub
scep.pubsub.credentials=/path/to/credentials.json

```

The `spanner-publisher.jar` contains a complete example of all possible
parameters in the root of the .jar named `scep.properties.example`.


## Support Level
Please feel free to report issues and send pull requests, but note that this
application is not officially supported as part of the Cloud Spanner product.
