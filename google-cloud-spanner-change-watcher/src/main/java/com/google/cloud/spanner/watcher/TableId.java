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

package com.google.cloud.spanner.watcher;

import com.google.api.client.util.Preconditions;
import com.google.cloud.spanner.DatabaseId;
import com.google.common.base.Strings;
import java.util.Objects;

/** Unique id of a Cloud Spanner table. */
public final class TableId {
  static class Builder {
    private final DatabaseId databaseId;
    private final String table;
    private String catalog = "";
    private String schema = "";

    private Builder(DatabaseId databaseId, String table) {
      this.databaseId = Preconditions.checkNotNull(databaseId);
      this.table = Preconditions.checkNotNull(table);
    }

    Builder setCatalog(String catalog) {
      this.catalog = Preconditions.checkNotNull(catalog);
      return this;
    }

    Builder setSchema(String schema) {
      this.schema = Preconditions.checkNotNull(schema);
      return this;
    }

    TableId build() {
      return new TableId(this);
    }
  }

  static Builder newBuilder(DatabaseId databaseId, String table) {
    return new Builder(databaseId, table);
  }

  /** Creates a {@link TableId} for the given table in the given database. */
  public static TableId of(DatabaseId databaseId, String table) {
    return newBuilder(databaseId, table).build();
  }

  private final DatabaseId databaseId;
  private final String catalog;
  private final String schema;
  private final String table;
  private final String sqlIdentifier;
  private final String fullName;

  private TableId(Builder builder) {
    this.databaseId = builder.databaseId;
    this.catalog = builder.catalog;
    this.schema = builder.schema;
    this.table = builder.table;
    StringBuilder id = new StringBuilder();
    if (!Strings.isNullOrEmpty(this.catalog)) {
      id.append("`").append(this.catalog).append("`.");
    }
    if (!Strings.isNullOrEmpty(this.schema)) {
      id.append("`").append(this.schema).append("`.");
    }
    id.append("`").append(this.table).append("`");
    this.sqlIdentifier = id.toString();
    this.fullName = databaseId.getName() + "/" + this.sqlIdentifier;
  }

  public DatabaseId getDatabaseId() {
    return databaseId;
  }

  public String getCatalog() {
    return catalog;
  }

  public String getSchema() {
    return schema;
  }

  public String getTable() {
    return table;
  }

  public String getSqlIdentifier() {
    return sqlIdentifier;
  }

  public String getFullName() {
    return fullName;
  }

  @Override
  public String toString() {
    return fullName;
  }

  @Override
  public int hashCode() {
    return Objects.hash(databaseId, catalog, schema, table);
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof TableId)) {
      return false;
    }
    TableId other = (TableId) o;
    return Objects.equals(databaseId, other.databaseId)
        && Objects.equals(catalog, other.catalog)
        && Objects.equals(schema, other.schema)
        && Objects.equals(table, other.table);
  }
}
