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

package com.google.cloud.spanner.publisher.sample;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.publisher.SpannerTableChangeEventPublisher;
import com.google.cloud.spanner.watcher.SpannerTableTailer;
import com.google.cloud.spanner.watcher.TableId;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;

@RunWith(JUnit4.class)
public class BuildTest {
  @Test
  public void testCanCreateWatcher() {
    SpannerTableChangeEventPublisher.newBuilder(
        SpannerTableTailer.newBuilder(
                Mockito.mock(Spanner.class), TableId.of(DatabaseId.of("p", "i", "d"), "t"))
            .build(),
        Mockito.mock(DatabaseClient.class));
  }
}
