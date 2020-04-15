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

package spannercdc

import (
	"context"
	"fmt"
	"os"

	"cloud.google.com/go/functions/metadata"
	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
)

// Archiver consumes a Pub/Sub message and writes it to an GCS Object.
func Archiver(ctx context.Context, msg pubsub.Message) error {
	if msg.Attributes["Replay"] == "True" {
		return nil
	}

	client, err := createStorageClient()
	if err != nil {
		return err
	}

	meta, err := metadata.FromContext(ctx)
	if err != nil {
		return err
	}
	bucketName, ok := os.LookupEnv("BUCKET_NAME")
	if !ok {
		return fmt.Errorf("no value for environment variable BUCKET_NAME defined")
	}
	fileName := msg.Attributes["Database"] + "/"
	if msg.Attributes["Catalog"] != "" {
		fileName = fileName + msg.Attributes["Catalog"] + "-"
	}
	if msg.Attributes["Schema"] != "" {
		fileName = fileName + msg.Attributes["Schema"] + "-"
	}
	fileName = fileName + msg.Attributes["Table"] + "-" + msg.Attributes["Timestamp"] + "-" + meta.EventID
	err = writeMessageToStorage(ctx, bucketName, client, fileName, msg)
	if err != nil {
		return err
	}

	return nil
}

type bucketHolder interface {
	Bucket(name string) *storage.BucketHandle
}

// client is the storage client to use. It is kept as a global variable so it
// can be reused for multiple function calls. It is defined as type
// bucketHolder so it can easily be mocked for testing.
var client bucketHolder

func createStorageClient() (bucketHolder, error) {
	if writeDataFunc == nil {
		writeDataFunc = writeData
	}
	if client == nil {
		var err error
		client, err = storage.NewClient(context.Background())
		if err != nil {
			return nil, fmt.Errorf("Failed to create client: %v", err)
		}
	}
	return client, nil
}

func writeMessageToStorage(
	ctx context.Context,
	bucketName string,
	client bucketHolder,
	fileName string,
	msg pubsub.Message,
) error {
	writer := client.Bucket(bucketName).Object(fileName).NewWriter(ctx)
	writer.ContentType = "protobuf/bytes"
	writer.Metadata = map[string]string{
		"Database": msg.Attributes["Database"],
		"Catalog": msg.Attributes["Catalog"],
		"Schema": msg.Attributes["Schema"],
		"Table": msg.Attributes["Table"],
		"Timestamp": msg.Attributes["Timestamp"],
	}
	return writeDataFunc(writer, msg, bucketName, fileName)
}

// writeDataFunc is the function that writes the actual data to Cloud Storage.
// It is stored as a global variable that is set during initialization to allow
// test code to set a mock function.
var writeDataFunc func(writer *storage.Writer, msg pubsub.Message, bucketName string, fileName string) error

func writeData(writer *storage.Writer, msg pubsub.Message, bucketName string, fileName string) error {
	if _, err := writer.Write(msg.Data); err != nil {
		return fmt.Errorf("Failed to write data to bucket %q, file %q: %v", bucketName, fileName, err)
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("Failed to close bucket %q, file %q: %v", bucketName, fileName, err)
	}
	return nil
}
