package spannercdc

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/functions/metadata"
	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
)

type testBucketHolder struct {
}

func (b *testBucketHolder) Bucket(name string) *storage.BucketHandle {
	return &storage.BucketHandle{}
}

func Test_Archiver(t *testing.T) {
	t.Parallel()

	os.Setenv("BUCKET_NAME", "cdc-test-bucket")
	ctx := metadata.NewContext(context.Background(), &metadata.Metadata{
		EventID:   "9999",
		Timestamp: time.Now(),
		EventType: "google.pubsub.topic.publish",
		Resource: &metadata.Resource{
			Service: "pubsub.googleapis.com",
			Name:    "projects/test-project/topics/test-topic",
			Type:    "google.pubsub.topic.publish",
			RawPath: "",
		},
	})
	msg := pubsub.Message{
		ID:              "9999",
		Data:            []byte("some data"),
		PublishTime:     time.Now(),
		Attributes: map[string]string{
			"Timestamp": "2020-04-05T21:23:59.199818000Z",
			"Database": "projects/test-project/instances/test-instance/databases/music",
			"Catalog": "",
			"Schema": "",
			"Table": "Singers",
		},
	}
	// Set the global client and write function variables.
	client = &testBucketHolder{}
	writeDataFunc = func(writer *storage.Writer, msg pubsub.Message, bucketName string, fileName string) error {
		if g, w := bucketName, "cdc-test-bucket"; g != w {
			return fmt.Errorf("bucket name mismatch\nGot: %s\nWant: %s", g, w)
		}
		if g, w := fileName, "projects/test-project/instances/test-instance/databases/music/Singers-2020-04-05T21:23:59.199818000Z-9999"; g != w {
			return fmt.Errorf("file name mismatch\nGot: %s\nWant: %s", g, w)
		}
		if g, w := string(msg.Data), "some data"; g != w {
			return fmt.Errorf("data mismatch\nGot: %s\nWant: %s", g, w)
		}
		if g, w := writer.ContentType, "protobuf/bytes"; g != w {
			return fmt.Errorf("content type mismatch\nGot: %s\nWant: %s", g, w)
		}
		if g, w := writer.Metadata["Database"], "projects/test-project/instances/test-instance/databases/music"; g != w {
			return fmt.Errorf("metadata database mismatch\nGot: %s\nWant: %s", g, w)
		}
		if g, w := writer.Metadata["Catalog"], ""; g != w {
			return fmt.Errorf("metadata catalog mismatch\nGot: %s\nWant: %s", g, w)
		}
		if g, w := writer.Metadata["Schema"], ""; g != w {
			return fmt.Errorf("metadata schema mismatch\nGot: %s\nWant: %s", g, w)
		}
		if g, w := writer.Metadata["Table"], "Singers"; g != w {
			return fmt.Errorf("metadata table mismatch\nGot: %s\nWant: %s", g, w)
		}
		if g, w := writer.Metadata["Timestamp"], "2020-04-05T21:23:59.199818000Z"; g != w {
			return fmt.Errorf("metadata timestamp mismatch\nGot: %s\nWant: %s", g, w)
		}
		return nil
	}
	err := Archiver(ctx, msg)
	if err != nil {
		t.Fatal(err)
	}
}
