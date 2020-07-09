# Spanner Change Publisher

## Introduction
Spanner Change Publisher watches a Spanner database for changes and publishes
these changes to Pubsub. It can monitor a single table, a set of tables, or all
tables in a database.

Spanner Change Publisher can be included in an existing application, or it can
be deployed as a standalone application.

A further introduction to Spanner Change Publisher [can be found here](https://medium.com/@knutolavloite/cloud-spanner-change-publisher-7fbee48f66f8).

## Usage in Existing Application
Clone, install and include the following dependency to your application to use
Spanner Change Publisher.

```
git clone git@github.com:cloudspannerecosystem/spanner-change-watcher.git
cd spanner-change-watcher
mvn clean install
```

```xml
<dependency>
  <groupId>com.google.cloud</groupId>
  <artifactId>google-cloud-spanner-change-publisher</artifactId>
  <version>0.1.0</version>
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

### Further Samples
Take a look at [Samples.java](../samples/spanner-change-publisher-samples/src/main/java/com/google/cloud/spanner/publisher/sample/Samples.java)
for additional examples of more advanced use cases.

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

# Converter factory to convert from Spanner to Pubsub.
scep.pubsub.converterFactory=com.google.cloud.spanner.publisher.SpannerToJsonFactory

```

The `spanner-publisher.jar` contains a complete example of all possible
parameters in the root of the .jar named `scep.properties.example`.


## Subscribe to Changes from Pubsub
Any application can subscribe to the changes that are published to Pubsub and
take any appropriate action based on the data that is received.

```java
String instance = "my-instance";
String database = "my-database";
String subscription = "projects/my-project/subscriptions/my-subscription";

// Create a Spanner client as we need this to decode the Avro records.
Spanner spanner = SpannerOptions.getDefaultInstance().getService();
DatabaseId databaseId = DatabaseId.of(SpannerOptions.getDefaultProjectId(), instance, database);
DatabaseClient client = spanner.getDatabaseClient(databaseId);

// Keep a cache of converters from Avro to Spanner as these are expensive to create.
Map<TableId, SpannerToAvro> converters = new HashMap<>();
// Start a subscriber.
Subscriber subscriber =
    Subscriber.newBuilder(
            ProjectSubscriptionName.of(pubsubProjectId, subscription),
            new MessageReceiver() {
              @Override
              public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
                // Get the change metadata.
                DatabaseId database = DatabaseId.of(message.getAttributesOrThrow("Database"));
                TableId table = TableId.of(database, message.getAttributesOrThrow("Table"));
                Timestamp commitTimestamp =
                    Timestamp.parseTimestamp(message.getAttributesOrThrow("Timestamp"));
                // Get the changed row and decode the data.
                SpannerToAvro converter = converters.get(table);
                if (converter == null) {
                  converter = new SpannerToAvro(client, table);
                }
                try {
                  GenericRecord record = converter.decodeRecord(message.getData());
                  System.out.println("--- Received changed record ---");
                  System.out.printf("Database: %s%n", database);
                  System.out.printf("Table: %s%n", table);
                  System.out.printf("Commit timestamp: %s%n", commitTimestamp);
                  System.out.printf("Data: %s%n", record);
                } catch (Exception e) {
                  System.err.printf("Failed to decode avro record: %s%n", e.getMessage());
                } finally {
                  consumer.ack();
                }
              }
            })
        .build();
subscriber.startAsync().awaitRunning();
```

## Limitations
* Spanner Change Publisher use [commit timestamps](https://cloud.google.com/spanner/docs/commit-timestamp) to determine when a
  change has occurred. They cannot be used on tables that do not include a commit timestamp.
* Deletes are not detected, unless these are soft deletes that only update a deleted flag in the corresponding table.
* Spanner Change Publisher polls tables for changes. Polling on larger tables can take some time and cause some delay
  before a change is detected. The default poll interval is 1 second and is configurable. It does support sharding to
  lower the load on large tables. Take a look at the samples for Spanner Change Watcher for more information.
* Spanner Change Publisher emits changes on a row level basis, including the commit timestamp of the change. It does not
  emit an even containing all changes of a single transaction. If that is needed, the client application will need to
  group the row level changes together based on the commit timestamp.
* Spanner Change Publisher is not a managed solution and does not come with Cloud Spanner's SLO. 

## Support Level
Please feel free to report issues and send pull requests, but note that this
application is not officially supported as part of the Cloud Spanner product.
