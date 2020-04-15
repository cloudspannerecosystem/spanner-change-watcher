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

package com.google.cloud.spanner.archiver.it;

import com.google.api.services.cloudfunctions.v1.CloudFunctionsScopes;
import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ServiceOptions;
import com.google.cloud.spanner.publisher.it.PubsubTestHelper.ITPubsubEnv;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Random;

/** Helper class for getting configuration values for integration tests. */
public class ArchiverTestHelper {
  public static class ITArchiverEnv extends ITPubsubEnv {
    final String functionId =
        System.getProperty(
            "functions.function", String.format("scw-test-function-%08d", RND.nextInt(100000000)));
    final String bucketName =
        System.getProperty(
            "storage.bucket", String.format("scw-test-changes-%08d", RND.nextInt(100000000)));
  }

  private static final Random RND = new Random();
  static final String FUNCTIONS_PROJECT_ID =
      System.getProperty("functions.project", ServiceOptions.getDefaultProjectId());
  private static final String FUNCTIONS_CREDENTIALS_FILE =
      System.getProperty("functions.credentials");
  static final String FUNCTIONS_SERVICE_ACCOUNT_EMAIL =
      System.getProperty("functions.serviceAccountEmail");

  static final String STORAGE_PROJECT_ID =
      System.getProperty("storage.project", ServiceOptions.getDefaultProjectId());
  private static final String STORAGE_CREDENTIALS_FILE = System.getProperty("storage.credentials");

  public static Credentials getCloudFunctionsCredentials() throws IOException {
    if (FUNCTIONS_CREDENTIALS_FILE != null) {
      return GoogleCredentials.fromStream(new FileInputStream(FUNCTIONS_CREDENTIALS_FILE))
          .createScoped(CloudFunctionsScopes.all());
    }
    return GoogleCredentials.getApplicationDefault().createScoped(CloudFunctionsScopes.all());
  }

  public static Credentials getStorageCredentials() throws IOException {
    if (STORAGE_CREDENTIALS_FILE != null) {
      return GoogleCredentials.fromStream(new FileInputStream(STORAGE_CREDENTIALS_FILE));
    }
    return GoogleCredentials.getApplicationDefault();
  }
}
