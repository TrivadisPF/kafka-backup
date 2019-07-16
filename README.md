# Kafka Backup

This is currently under development ....

Key Features:

 * ...
 * 
##### Backup retention

Each topic on Kafka when it is defined contains as a property the retention about the records stored inside it. This property allow Kafka to purge the data with older records outside the topic.
During the backup in order to have a good dimension of the S3 bucket (not a huge bucket), it is possible to implement a the similar mechanism of kakfa allowed by S3 itself.

What:
 - Define a lifecycle for specific prefix inside the bucket. In our case the prefix name will be the name of the topic (bucket-name/topic-name/partition/object).
 - Apply/update the S3 object lifecycle when the connector will be defined on Kafka-Connect.
 - Only deletion of the file will be available. No other backup will be maintained.

How:
 - The connector configuration will contains the information related to the retention. There will be a default value if the user not define nothing.
 - Apply/update the lifecycle using the AWS-SDK as described here https://docs.aws.amazon.com/AmazonS3/latest/dev/manage-lifecycle-using-java.html).


## Changelog

 * 0.x.x


## Documentation

The documentation is available [here](doc/README.md).

### Starting the Backup
The Backup connectors are processes which execute a copy of the data/metadata contained inside a topic to an S3 bucket.
On Kafka there are two types of topics (compacted topic and not compacted topic). The different between the two topics consist (in a compacted topic)
to allow on Kafka process to cleanup records with the same key and to leave only the last record inserted (temporary). Inside the normal topic there are no other changes.
All records will be maintained inside the topic. There is also the retention policy which remove the old data and the mechanism works for both typology of topics.

####Backup Normal topic

#####Assumptions
The S3 bucket need to be empty. In other case the process will override the files.
The data will be stored using the following naming conventions "BUCKET_NAME/TOPIC_NAME/PARTITION_NUMBER/TOPIC_NAME-RECORD_PARTITION-RECORD_OFFSET.avro"
The properties s3.profile.name is optional. It represent the name of the profile to use inside the credentials file for AWS
It is possible to configure the buffer of the data to upload on S3, using a number of records (flush.size) or by interval (rotate.interval.ms)

The following curl is an example of the configuration to a normal backup of topic.

```
curl -X PUT http://localhost:8084/connectors/backup/config \
     -H 'Content-Type: application/json' \
     -H 'Accept: application/json' \
     -d '{
	     "connector.class":"ch.tbd.kafka.backuprestore.backup.kafkaconnect.BackupSinkConnector", 
	     "value.converter": "org.apache.kafka.connect.converters.ByteArrayConverter",
	     "key.converter":"org.apache.kafka.connect.converters.ByteArrayConverter", 
	     "topics":"test-topic", 
	     "flush.size":"3", 
	     "s3.bucket.name":"TBD", 
	     "s3.region":"eu-central-1", 
	     "s3.proxy.url": "TBD", 
	     "tasks.max":"1"
     }'
```

####Backup Compacted topic
As described before using a compacted topic, Kafka provide to cleanup the data with multiple key asynchronously. This is not a problem for a backup process but it is
relevant when try to restore the data. I could use more time than I need. The problem is explained better [here](doc/README.md).

The idea is to start two processes (ACTIVATE/PASSIVATE) which some times change the status and restart the backup on S3. In this case I will have a snapshot with the latest keys. 

#####Assumptions
The S3 bucket need to be empty. In other case the process will override the files. When the system change the status from PASSIVATE to ACTIVATE automatically it will clean the S3 bucket before to start the backup.
The data will be stored using the following naming conventions "BUCKET_NAME/TOPIC_NAME/BACKUP_INSTANCE/PARTITION_NUMBER/TOPIC_NAME-RECORD_PARTITION-RECORD_OFFSET.avro"
The properties s3.profile.name is optional. It represent the name of the profile to use inside the credentials file for AWS
It is possible to configure the buffer of the data to upload on S3, using a number of records (flush.size) or by interval (rotate.interval.ms)

The following curl is an example of the configuration to a normal backup of topic.
compacted.log.backup.length.hours     --> Interval after which time go to PASSIVATE
compacted.log.backup.interval.offsets --> Number of offsets after the system communicate to the PASSIVATE task the new latest offset stored on S3
compacted.log.backup.path.configuration --> Kafka configuration file. This file is important in order to define a consumers/producers to write and read messages from the coordination topic
compacted.log.backup.login.name.jaas.configuration --> In case the system use the jaas file for the authentication, this is the loginmodulename to use inside the producer and consumer properties.
```

curl -X PUT http://localhost:8084/connectors/backup-activate/config 
     -H 'Content-Type: application/json' 
     -H 'Accept: application/json' 
     --data '{
        "connector.class":"ch.tbd.kafka.backuprestore.backup.kafkaconnect.compact.CompactBackupSinkConnector", 
        "value.converter": "org.apache.kafka.connect.converters.ByteArrayConverter", 
        "key.converter": "org.apache.kafka.connect.converters.ByteArrayConverter", 
        "topics": "test-topic", 
        "flush.size": "3", 
        "s3.bucket.name": "TBD", 
        "s3.region": "TBD", 
        "s3.proxy.url": "TBD", 
        "compacted.log.backup.initial.status":"ACTIVATE", 
        "tasks.max":"1",
        "compacted.log.backup.length.hours":"1",
        "compacted.log.backup.interval.offsets": "10000",
        "compacted.log.backup.path.configuration":"/etc/kafka-connect/kafka-connect.properties",
        "compacted.log.backup.login.name.jaas.configuration": "LoginModuleName"}'

curl -X PUT http://localhost:8084/connectors/backup-passivate/config 
     -H 'Content-Type: application/json' 
     -H 'Accept: application/json' 
     --data '{
        "connector.class":"ch.tbd.kafka.backuprestore.backup.kafkaconnect.compact.CompactBackupSinkConnector", 
        "value.converter": "org.apache.kafka.connect.converters.ByteArrayConverter", 
        "key.converter": "org.apache.kafka.connect.converters.ByteArrayConverter", 
        "topics": "test-topic", 
        "flush.size": "3", 
        "s3.bucket.name": "TBD", 
        "s3.region": "TBD", 
        "s3.proxy.url": "TBD", 
        "compacted.log.backup.initial.status":"PASSIVATE", 
        "tasks.max":"1",
        "compacted.log.backup.length.hours":"1",
        "compacted.log.backup.interval.offsets": "10000",
        "compacted.log.backup.path.configuration":"/etc/kafka-connect/kafka-connect.properties",
        "compacted.log.backup.login.name.jaas.configuration": "LoginModuleName"}'
```

### Starting a Restore 
The restore process get the data/metadata from S3 bucket and store it on kafka topic. It is possible to define how many tasks to activate (max num task === total number partitions).

####Assumptions
The topics exist and have the same configuration about the data to restore (same number of partitions, same properties,...)
The properties s3.profile.name is optional. It represent the name of the profile to use inside the credentials file for AWS
The property instance.name.restore is optional and it is used in case the backup it is related to the compacted topics. In this case need to put explicitly the name of the instance which want to restore.

```
curl -X PUT http://localhost:8084/connectors/source-restore/config \
     -H 'Content-Type: application/json' \
     -H 'Accept: application/json' \
     --data '{
     	"connector.class": "ch.tbd.kafka.backuprestore.restore.kafkaconnect.RestoreSourceConnector", 
     	"tasks.max":"1", 
     	"s3.region":"eu-central-1", 
     	"value.converter": "org.apache.kafka.connect.converters.ByteArrayConverter", 
     	"key.converter":"org.apache.kafka.connect.converters.ByteArrayConverter", 
     	"s3.proxy.url": "TBD", 
     	"s3.bucket.name":"TBD", 
     	"topics":"test-topic"
     	}'
```

```
curl -X PUT http://localhost:8084/connectors/source-restore/config \
     -H 'Content-Type: application/json' \
     -H 'Accept: application/json' \
     --data '{
        "connector.class": "ch.tbd.kafka.backuprestore.restore.kafkaconnect.RestoreSourceConnector", 
        "tasks.max":"1", 
        "s3.region":"eu-central-1", 
        "value.converter": "org.apache.kafka.connect.converters.ByteArrayConverter", 
        "key.converter":"org.apache.kafka.connect.converters.ByteArrayConverter", 
        "s3.proxy.url": "TBD", 
        "s3.bucket.name":"TBD",  
        "topics":"test-topic:new-topic"}'
```


```
curl -X PUT http://localhost:8084/connectors/source-restore/config \
     -H 'Content-Type: application/json' \
     -H 'Accept: application/json' \
     --data '{
        "connector.class": "ch.tbd.kafka.backuprestore.restore.kafkaconnect.RestoreSourceConnector", 
        "tasks.max":"1", 
        "s3.region":"eu-central-1", 
        "value.converter": "org.apache.kafka.connect.converters.ByteArrayConverter", 
        "key.converter":"org.apache.kafka.connect.converters.ByteArrayConverter", 
        "s3.proxy.url": "TBD", 
        "s3.bucket.name":"TBD", 
        "topics":"test-topic:new-topic,test-topic1:new-topic1,test-topic2:new-topic2"}'
```


