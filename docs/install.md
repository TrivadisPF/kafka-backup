# Kafka Backup & Restore

## Installation Guide

The connector is available from the release page of this GitHub Project.

The uber jar (`kafka-backup-restore-00.01.00.01-SNAPSHOT.jar`) contains all the dependencies required for the connector.

To install:

  * On the Kafka Connector machine copy the uber jar into the plugins directory: /usr/local/share/kafka/plugins/
  * Configure the connector.
    * The `KafkaBackupConnector.properties` provides an example configuration for the Kafka Backup (Sink) connector. Where data is written from one or more topics into Amazon S3.
    * The `KafkaRestoreConnector.properties` provides an example configuration for the Kafka Restore (Source) connector. Where data is restored from Amazone S3 into Kafka topics

 
For more information on installing connectors see the official Confluent documentation.

 * [Manually installing community connectors](https://docs.confluent.io/5.3.0/connect/managing/community.html) and
 * [Configuring connectors](https://docs.confluent.io/5.3.0/connect/managing/configuring.html) for more information.

-------
### Next

  * Installation Guide
  * [The Kafka Backup connector guide](kafka-backup.md)
  * The Kafka Restore connector guide
  * A docker end-to-end example
  * Changelog
