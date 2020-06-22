package com.google.cloud.spanner.watcher.sample;

import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.DatabaseNotFoundException;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Value;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class SampleData {
  /**
   * Sample table definitions. Both tables include a CommitTimestamp column with OPTIONS
   * allow_commit_timestamp=true.
   */
  static final ImmutableList<String> CREATE_TABLE_STATEMENTS =
      ImmutableList.of(
          "CREATE TABLE Singers (\n"
              + "  SingerId INT64,\n"
              + "  FirstName STRING(MAX),\n"
              + "  LastName STRING(MAX),\n"
              + "  LastUpdateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true)\n"
              + ") PRIMARY KEY (SingerId)",
          "CREATE TABLE Albums (\n"
              + "  SingerId INT64,\n"
              + "  AlbumId INT64,\n"
              + "  AlbumTitle STRING(MAX),\n"
              + "  LastUpdateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true)\n"
              + ") PRIMARY KEY (SingerId, AlbumId),\n"
              + "INTERLEAVE IN PARENT Singers ON DELETE CASCADE");

  /** Class to contain singer sample data. */
  static class Singer {
    final long singerId;
    final String firstName;
    final String lastName;

    Singer(long singerId, String firstName, String lastName) {
      this.singerId = singerId;
      this.firstName = firstName;
      this.lastName = lastName;
    }
  }

  /** Class to contain album sample data. */
  static class Album {
    final long singerId;
    final long albumId;
    final String albumTitle;

    Album(long singerId, long albumId, String albumTitle) {
      this.singerId = singerId;
      this.albumId = albumId;
      this.albumTitle = albumTitle;
    }
  }

  static final List<Singer> SINGERS =
      Arrays.asList(
          new Singer(1, "Marc", "Richards"),
          new Singer(2, "Catalina", "Smith"),
          new Singer(3, "Alice", "Trentor"),
          new Singer(4, "Lea", "Martin"),
          new Singer(5, "David", "Lomond"));

  static final List<Album> ALBUMS =
      Arrays.asList(
          new Album(1, 1, "Total Junk"),
          new Album(1, 2, "Go, Go, Go"),
          new Album(2, 1, "Green"),
          new Album(2, 2, "Forever Hold Your Peace"),
          new Album(2, 3, "Terrified"));

  /** Creates the sample database and sample tables. */
  static void createSampleDatabase(Spanner spanner, DatabaseId databaseId) {
    DatabaseAdminClient dbAdminClient = spanner.getDatabaseAdminClient();
    try {
      dbAdminClient.getDatabase(databaseId.getInstanceId().getInstance(), databaseId.getDatabase());
      DatabaseClient dbClient = spanner.getDatabaseClient(databaseId);
      try (ResultSet rs =
          dbClient
              .singleUse()
              .executeQuery(
                  Statement.of(
                      "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME IN ('Singers', 'Albums')"))) {
        if (rs.next()) {
          return;
        }
      }
      try {
        dbAdminClient
            .updateDatabaseDdl(
                databaseId.getInstanceId().getInstance(),
                databaseId.getDatabase(),
                CREATE_TABLE_STATEMENTS,
                null)
            .get();
      } catch (InterruptedException e) {
        throw SpannerExceptionFactory.propagateInterrupt(e);
      } catch (ExecutionException e) {
        throw SpannerExceptionFactory.newSpannerException(e.getCause());
      }
    } catch (DatabaseNotFoundException e) {
      try {
        dbAdminClient
            .createDatabase(
                databaseId.getInstanceId().getInstance(),
                databaseId.getDatabase(),
                CREATE_TABLE_STATEMENTS)
            .get();
      } catch (InterruptedException e1) {
        throw SpannerExceptionFactory.propagateInterrupt(e1);
      } catch (ExecutionException e1) {
        throw SpannerExceptionFactory.newSpannerException(e1.getCause());
      }
    }
  }

  /** Write some sample data to the database. */
  static void writeExampleData(DatabaseClient dbClient) {
    List<Mutation> mutations = new ArrayList<>();
    for (Singer singer : SINGERS) {
      mutations.add(
          Mutation.newInsertOrUpdateBuilder("Singers")
              .set("SingerId")
              .to(singer.singerId)
              .set("FirstName")
              .to(singer.firstName)
              .set("LastName")
              .to(singer.lastName)
              .set("LastUpdateTime")
              .to(Value.COMMIT_TIMESTAMP)
              .build());
    }
    dbClient.write(mutations);

    mutations.clear();
    for (Album album : ALBUMS) {
      mutations.add(
          Mutation.newInsertOrUpdateBuilder("Albums")
              .set("SingerId")
              .to(album.singerId)
              .set("AlbumId")
              .to(album.albumId)
              .set("AlbumTitle")
              .to(album.albumTitle)
              .set("LastUpdateTime")
              .to(Value.COMMIT_TIMESTAMP)
              .build());
    }
    dbClient.write(mutations);
  }
}
