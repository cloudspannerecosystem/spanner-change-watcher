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

import static com.google.common.truth.Truth.assertThat;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.ByteArrayContent;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.cloudfunctions.v1.CloudFunctions;
import com.google.api.services.cloudfunctions.v1.model.GenerateUploadUrlRequest;
import com.google.api.services.cloudfunctions.v1.model.GenerateUploadUrlResponse;
import com.google.auth.http.HttpCredentialsAdapter;
import java.io.ByteArrayOutputStream;
import java.nio.charset.Charset;
import java.util.Scanner;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@Ignore("needs additional permissions")
@RunWith(JUnit4.class)
public class ITUploaderTest {
  private static CloudFunctions functions;
  private static String functionLocation = "us-east1";

  @BeforeClass
  public static void setup() throws Exception {
    functions =
        new CloudFunctions(
            GoogleNetHttpTransport.newTrustedTransport(),
            JacksonFactory.getDefaultInstance(),
            new HttpCredentialsAdapter(ArchiverTestHelper.getCloudFunctionsCredentials()));
  }

  @Test
  public void testUploadSourceCode() throws Exception {
    Charset utf8 = Charset.forName("UTF8");
    ByteArrayOutputStream zippedCode = new ByteArrayOutputStream(1024);
    ZipOutputStream zos = new ZipOutputStream(zippedCode);
    for (String entry : new String[] {"archiver.go", "go.mod"}) {
      zos.putNextEntry(new ZipEntry(entry));
      try (Scanner scanner =
          new Scanner(getClass().getClassLoader().getResourceAsStream("go/" + entry))) {
        while (scanner.hasNextLine()) {
          zos.write(scanner.nextLine().getBytes(utf8));
          zos.write("\n".getBytes(utf8));
        }
      }
      zos.closeEntry();
    }
    zos.close();

    GenerateUploadUrlResponse response =
        functions
            .projects()
            .locations()
            .functions()
            .generateUploadUrl(
                String.format(
                    "projects/%s/locations/%s",
                    ArchiverTestHelper.FUNCTIONS_PROJECT_ID, functionLocation),
                new GenerateUploadUrlRequest())
            .execute();
    NetHttpTransport transport = GoogleNetHttpTransport.newTrustedTransport();
    HttpRequestFactory requestFactory = transport.createRequestFactory();
    HttpRequest request =
        requestFactory.buildPutRequest(
            new GenericUrl(response.getUploadUrl()),
            new ByteArrayContent("application/zip", zippedCode.toByteArray()));
    request.setHeaders(new HttpHeaders().set("x-goog-content-length-range", "0,104857600"));
    HttpResponse uploadResponse = request.execute();
    assertThat(uploadResponse.isSuccessStatusCode()).isTrue();
  }
}
