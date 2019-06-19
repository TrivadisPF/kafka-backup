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

### Starting the Backupt

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
	     "s3.proxy.port": "TBD",
	     "tasks.max":"1"
     }'
```

### Starting a Restore 

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
     	"s3.proxy.port": "TBD", 
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
        "s3.proxy.port": "TBD", 
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
        "s3.proxy.port": "TBD", 
        "topics":"test-topic:new-topic,test-topic1:new-topic1,test-topic2:new-topic2"}'
```


