# Spanner Change Watcher

## Introduction
Spanner Change Watcher watches Spanner databases and tables for changes and
emits events when changes are detected. This framework can be included in an
existing application and used to trigger functionality or processes in that
application based on data change events.

## Example Usage
Spanner Change Watcher can be used to watch both single tables, a set of tables,
or entire databases. Clone, install and add the dependency to your project:

```
git clone git@github.com:cloudspannerecosystem/spanner-change-watcher.git
cd spanner-change-watcher
mvn clean install
```

```xml
<dependency>
  <groupId>com.google.cloudspannerecosystem</groupId>
  <artifactId>google-cloud-spanner-change-watcher</artifactId>
  <version>0.1.0</version>
</dependency>
```

### Watch Single Table
Watches a single table in a Spanner database for changes.

```java
String instance = "my-instance";
String database = "my-database";
String table = "MY_TABLE";

Spanner spanner = SpannerOptions.getDefaultInstance().getService();
TableId tableId =
    TableId.of(DatabaseId.of(SpannerOptions.getDefaultProjectId(), instance, database), table);
SpannerTableChangeWatcher watcher = SpannerTableTailer.newBuilder(spanner, tableId).build();
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

### Watch Entire Database
Watches all tables in a Spanner database for changes.

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

### Further Samples
Take a look at [Samples.java](../samples/spanner-change-watcher-samples/src/main/java/com/google/cloud/spanner/watcher/sample/Samples.java)
for additional examples of more advanced use cases.


## Support Level
Please feel free to report issues and send pull requests, but note that this
application is not officially supported as part of the Cloud Spanner product.
