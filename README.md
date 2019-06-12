# Kafka Backup

Key Features:

 * ...
 * 


## Changelog

 * 0.x.x


## Documentation

The documentation is available [here](doc/README.md).

EXAMPLE SINK BACKUP CONNECTOR
```
curl -X PUT http://localhost:8084/connectors/backup/config -H 'Content-Type: application/json' -H 'Accept: application/json' --data '{"connector.class":"ch.tbd.kafka.backuprestore.backup.kafkaconnect.BackupSinkConnector", "value.converter": "org.apache.kafka.connect.converters.ByteArrayConverter", "key.converter":"org.apache.kafka.connect.converters.ByteArrayConverter", "topics":"test-topic", "flush.size":"3", "s3.bucket.name":"TBD", "s3.region":"eu-central-1", "s3.proxy.url": "TBD", "s3.proxy.port": "TBD"}'

```
EXAMPLE SOURCE RESTORE CONNECTOR

```
curl -X PUT http://localhost:8084/connectors/source-restore/config -H 'Content-Type: application/json' -H 'Accept: application/json' --data '{"connector.class": "ch.tbd.kafka.backuprestore.restore.kafkaconnect.RestoreSourceConnector", "tasks.max":"1", "s3.region":"eu-central-1", "value.converter": "org.apache.kafka.connect.converters.ByteArrayConverter", "key.converter":"org.apache.kafka.connect.converters.ByteArrayConverter", "s3.proxy.url": "TBD", "s3.bucket.name":"TBD", "s3.proxy.port": "TBD", "topic.name":"test-topic"}'
```

```
curl -X PUT http://localhost:8084/connectors/source-restore/config -H 'Content-Type: application/json' -H 'Accept: application/json' --data '{"connector.class": "ch.tbd.kafka.backuprestore.restore.kafkaconnect.RestoreSourceConnector", "tasks.max":"1", "s3.region":"eu-central-1", "value.converter": "org.apache.kafka.connect.converters.ByteArrayConverter", "key.converter":"org.apache.kafka.connect.converters.ByteArrayConverter", "s3.proxy.url": "TBD", "s3.bucket.name":"TBD", "s3.proxy.port": "TBD", "topic.name":"test-topic:new-topic"}'
```



