# Spanner Change Archiver
Spanner Change Archiver is an example application using a Spanner Change
Publisher to publish changes in a Google Cloud Spanner database to a Pubsub
topic. This topic is used as a trigger for a Google Cloud Function that writes
the detected change to Google Cloud Storage. This creates a complete change log
for the database in files stored in Google Cloud Storage.

This example project can also be used as a template for other use cases that need
to trigger a Google Cloud Function based on data change events in a Cloud Spanner
database. 

## Support Level
Please feel free to report issues and send pull requests, but note that this
application is not officially supported as part of the Cloud Spanner product.
