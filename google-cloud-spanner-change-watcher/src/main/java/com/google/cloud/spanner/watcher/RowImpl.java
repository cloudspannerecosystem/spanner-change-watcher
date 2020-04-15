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

import com.google.cloud.spanner.ForwardingStructReader;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.watcher.SpannerTableChangeWatcher.Row;

/** {@link Row} implementation using a {@link ResultSet} as its backing data source. */
class RowImpl extends ForwardingStructReader implements Row {
  private final ResultSet delegate;

  RowImpl(ResultSet delegate) {
    super(delegate);
    this.delegate = delegate;
  }

  @Override
  public Struct asStruct() {
    return delegate.getCurrentRowAsStruct();
  }
}
