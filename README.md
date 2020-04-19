# Google Cloud Spanner Change Watcher and Publisher

*Under construction, please check back later!*

## Introduction
Google Cloud Spanner Change Watcher and Publisher is a framework that emits
events when a row has been changed in a Cloud Spanner database. These row
change events can be captured and processed in-process as part of a user
application, or they can be published to Google Cloud Pubsub for consumption by
any other application.

## Spanner Change Watcher
Spanner Change Watcher is the core framework that watches Spanner databases and
table for changes and emits events when these are detected. This framework can
be included in a user application and used to trigger functionality or
processes in that application based on data change events.

See [Spanner Change Watcher README file](./google-cloud-spanner-change-watcher/README.md)
for more information.

## Spanner Change Publisher
Spanner Change Publisher publishes row changes in a Cloud Spanner database to
Google Cloud Pubsub. The change events can be consumed by any other application
subscribing to the topic(s) used by the Spanner Change Publisher.

Spanner Change Publisher can be executed as a stand-alone application or it can
be included as a dependency in an existing user application and be managed by
that application.

See [Spanner Change Publisher README file](./google-cloud-spanner-change-publisher/README.md)
for more information.

## Spanner Change Archiver
Spanner Change Archiver is an example application using a Spanner Change
Publisher to publish changes in a Google Cloud Spanner database to a Pubsub
topic. This topic is used as a trigger for a Google Cloud Function that writes
the detected change to Google Cloud Storage. This creates a complete change log
for the database in files stored in Google Cloud Storage.

See [Spanner Change Archiver README file](./google-cloud-spanner-change-archiver/README.md)
for more information.

## Support Level
Please feel free to report issues and send pull requests, but note that this
application is not officially supported as part of the Cloud Spanner product.
