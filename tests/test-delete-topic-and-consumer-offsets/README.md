# Testing Deleting a topic while "backup" is running

## Prepare

Create a new topic `test-topic`

```
kafka-topics  --zookeeper zookeeper-1:2181 --create --topic test-topic --replication-factor 3 --partitions 3
```

Generate data using the following Python producer

```
import time
from confluent_kafka import Producer

p = Producer({'bootstrap.servers': 'broker-1:9092,broker-2:9093'})
messages = ["message1","message2","message3"]

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

for i in range(10000000):
    for data in messages:
       # Trigger any available delivery report callbacks from previous produce() calls
       p.poll(0)

       # Asynchronously produce a message, the delivery report callback
       # will be triggered from poll() above, or flush() below, when the message has
       # been successfully delivered or failed permanently.
       p.produce('test-topic'
             , key=str(i)
             , value = data.encode('utf-8')
             , callback=delivery_report)

    # Wait for any outstanding messages to be delivered and delivery report
    # callbacks to be triggered.
    p.flush()
    time.sleep(.300)
```

Start "backup" using Confluent S3 Connector

```
curl -X "POST" "$DOCKER_HOST_IP:8083/connectors" \
     -H "Content-Type: application/json" \
     --data '{
  "name": "s3-confluent-sink",
  "config": {
      "connector.class": "io.confluent.connect.s3.S3SinkConnector",
      "partition.duration.ms": "3600000",
      "flush.size": "100",
      "topics": "test-topic",
      "tasks.max": "1",
      "timezone": "UTC",
      "locale": "en",
      "partitioner.class": "io.confluent.connect.storage.partitioner.DefaultPartitioner",
      "schema.generator.class": "io.confluent.connect.storage.hive.schema.DefaultSchemaGenerator",
      "storage.class": "io.confluent.connect.s3.storage.S3Storage",
      "format.class": "io.confluent.connect.s3.format.json.JsonFormat",
      "s3.region": "us-east-1",
      "s3.bucket.name": "gschmutz-kafka-confluent-1",
      "s3.part.size": "5242880",
      "store.url": "http://minio:9000",
      "key.converter": "org.apache.kafka.connect.storage.StringConverter",
      "value.converter": "org.apache.kafka.connect.storage.StringConverter"
  }
}'
```

Consume topic `__consumer_offsets`

```
kafka-console-consumer --formatter "kafka.coordinator.group.GroupMetadataManager\$OffsetsMessageFormatter" --topic __consumer_offsets --bootstrap-server localhost:9092
```

and you see offsets being committed

```
[connect-s3-confluent-sink,test-topic,0]::OffsetAndMetadata(offset=200, leaderEpoch=Optional.empty, metadata=, commitTimestamp=1559544947143, expireTimestamp=None)
[connect-s3-confluent-sink,test-topic,1]::OffsetAndMetadata(offset=200, leaderEpoch=Optional.empty, metadata=, commitTimestamp=1559544947143, expireTimestamp=None)
[connect-s3-confluent-sink,test-topic,2]::OffsetAndMetadata(offset=200, leaderEpoch=Optional.empty, metadata=, commitTimestamp=1559544967143, expireTimestamp=None)
[connect-s3-confluent-sink,test-topic,0]::OffsetAndMetadata(offset=300, leaderEpoch=Optional.empty, metadata=, commitTimestamp=1559544967143, expireTimestamp=None)
[connect-s3-confluent-sink,test-topic,1]::OffsetAndMetadata(offset=300, leaderEpoch=Optional.empty, metadata=, commitTimestamp=1559544967143, expireTimestamp=None)
```

## Test

Delete the topic

```
kafka-topics --zookeeper zookeeper-1:2181 --delete --topic test-topic
```

A message with a `NULL` value will be written to the `__consumer_offsets` topic

```
root@broker-1:/# kafka-console-consumer --formatter "kafka.coordinator.group.GroupMetadataManager\$OffsetsMessageFormatter" --topic __consumer_offsets --bootstrap-server localhost:9092
[connect-s3-confluent-sink,test-topic,0]::OffsetAndMetadata(offset=200, leaderEpoch=Optional.empty, metadata=, commitTimestamp=1559544947143, expireTimestamp=None)
[connect-s3-confluent-sink,test-topic,1]::OffsetAndMetadata(offset=200, leaderEpoch=Optional.empty, metadata=, commitTimestamp=1559544947143, expireTimestamp=None)
[connect-s3-confluent-sink,test-topic,2]::OffsetAndMetadata(offset=200, leaderEpoch=Optional.empty, metadata=, commitTimestamp=1559544967143, expireTimestamp=None)
[connect-s3-confluent-sink,test-topic,0]::OffsetAndMetadata(offset=300, leaderEpoch=Optional.empty, metadata=, commitTimestamp=1559544967143, expireTimestamp=None)
[connect-s3-confluent-sink,test-topic,1]::OffsetAndMetadata(offset=300, leaderEpoch=Optional.empty, metadata=, commitTimestamp=1559544967143, expireTimestamp=None)
[connect-s3-confluent-sink,test-topic,2]::NULL
[connect-s3-confluent-sink,test-topic,0]::NULL
[connect-s3-confluent-sink,test-topic,1]::NULL
```

the connector gets an error

```
connect-1             | [2019-06-03 06:56:02,478] INFO Starting commit and rotation for topic partition test-topic-1 with start offset {partition=1=200} (io.confluent.connect.s3.TopicPartitionWriter)
connect-1             | [2019-06-03 06:56:02,494] INFO Files committed to S3. Target commit offset for test-topic-1 is 300 (io.confluent.connect.s3.TopicPartitionWriter)
connect-1             | [2019-06-03 06:56:07,092] INFO Starting commit and rotation for topic partition test-topic-0 with start offset {partition=0=200} (io.confluent.connect.s3.TopicPartitionWriter)
connect-1             | [2019-06-03 06:56:07,108] INFO Files committed to S3. Target commit offset for test-topic-0 is 300 (io.confluent.connect.s3.TopicPartitionWriter)
connect-1             | [2019-06-03 06:56:07,142] INFO WorkerSinkTask{id=s3-confluent-sink-0} Committing offsets asynchronously using sequence number 21: {test-topic-2=OffsetAndMetadata{offset=200, leaderEpoch=null, metadata=''}, test-topic-0=OffsetAndMetadata{offset=300, leaderEpoch=null, metadata=''}, test-topic-1=OffsetAndMetadata{offset=300, leaderEpoch=null, metadata=''}} (org.apache.kafka.connect.runtime.WorkerSinkTask)
connect-1             | [2019-06-03 06:56:16,921] INFO Starting commit and rotation for topic partition test-topic-2 with start offset {partition=2=200} (io.confluent.connect.s3.TopicPartitionWriter)
connect-1             | [2019-06-03 06:56:16,965] INFO Files committed to S3. Target commit offset for test-topic-2 is 300 (io.confluent.connect.s3.TopicPartitionWriter)
connect-1             | [2019-06-03 06:56:17,114] WARN [Consumer clientId=consumer-9, groupId=connect-s3-confluent-sink] Received unknown topic or partition error in fetch for partition test-topic-0 (org.apache.kafka.clients.consumer.internals.Fetcher)
connect-1             | [2019-06-03 06:56:17,118] WARN [Consumer clientId=consumer-9, groupId=connect-s3-confluent-sink] Error while fetching metadata with correlation id 2177 : {test-topic=UNKNOWN_TOPIC_OR_PARTITION} (org.apache.kafka.clients.NetworkClient)
connect-1             | [2019-06-03 06:56:17,119] INFO [Consumer clientId=consumer-9, groupId=connect-s3-confluent-sink] Revoking previously assigned partitions [test-topic-2, test-topic-0, test-topic-1] (org.apache.kafka.clients.consumer.internals.ConsumerCoordinator)
connect-1             | [2019-06-03 06:56:17,120] INFO WorkerSinkTask{id=s3-confluent-sink-0} Committing offsets synchronously using sequence number 22: {test-topic-2=OffsetAndMetadata{offset=300, leaderEpoch=null, metadata=''}, test-topic-0=OffsetAndMetadata{offset=300, leaderEpoch=null, metadata=''}, test-topic-1=OffsetAndMetadata{offset=300, leaderEpoch=null, metadata=''}} (org.apache.kafka.connect.runtime.WorkerSinkTask)
connect-1             | [2019-06-03 06:56:17,122] ERROR [Consumer clientId=consumer-9, groupId=connect-s3-confluent-sink] Offset commit failed on partition test-topic-2 at offset 300: This server does not host this topic-partition. (org.apache.kafka.clients.consumer.internals.ConsumerCoordinator)
connect-1             | [2019-06-03 06:56:17,223] WARN [Consumer clientId=consumer-9, groupId=connect-s3-confluent-sink] Error while fetching metadata with correlation id 2180 : {test-topic=UNKNOWN_TOPIC_OR_PARTITION} (org.apache.kafka.clients.NetworkClient)
connect-1             | [2019-06-03 06:56:17,223] ERROR [Consumer clientId=consumer-9, groupId=connect-s3-confluent-sink] Offset commit failed on partition test-topic-2 at offset 300: This server does not host this topic-partition. (org.apache.kafka.clients.consumer.internals.ConsumerCoordinator)
connect-1             | [2019-06-03 06:56:17,325] WARN [Consumer clientId=consumer-9, groupId=connect-s3-confluent-sink] Error while fetching metadata with correlation id 2182 : {test-topic=UNKNOWN_TOPIC_OR_PARTITION} (org.apache.kafka.clients.NetworkClient)
connect-1             | [2019-06-03 06:56:17,325] ERROR [Consumer clientId=consumer-9, groupId=connect-s3-confluent-sink] Offset commit failed on partition test-topic-2 at offset 300: This server does not host this topic-partition. (org.apache.kafka.clients.consumer.internals.ConsumerCoordinator)
connect-1             | [2019-06-03 06:56:17,426] ERROR [Consumer clientId=consumer-9, groupId=connect-s3-confluent-sink] Offset commit failed on partition test-topic-2 at offset 300: This server does not host this topic-partition. (org.apache.kafka.clients.consumer.internals.ConsumerCoordinator)
connect-1             | [2019-06-03 06:56:17,513] WARN [Consumer clientId=consumer-9, groupId=connect-s3-confluent-sink] Error while fetching metadata with correlation id 2184 : {test-topic=UNKNOWN_TOPIC_OR_PARTITION} (org.apache.kafka.clients.NetworkClient)
connect-1             | [2019-06-03 06:56:17,528] ERROR [Consumer clientId=consumer-9, groupId=connect-s3-confluent-sink] Offset commit failed on partition test-topic-2 at offset 300: This server does not host this topic-partition. (org.apache.kafka.clients.consumer.internals.ConsumerCoordinator)
connect-1             | [2019-06-03 06:56:17,629] WARN [Consumer clientId=consumer-9, groupId=connect-s3-confluent-sink] Error while fetching metadata with correlation id 2186 : {test-topic=UNKNOWN_TOPIC_OR_PARTITION} (org.apache.kafka.clients.NetworkClient)
connect-1             | [2019-06-03 06:56:17,629] ERROR [Consumer clientId=consumer-9, groupId=connect-s3-confluent-sink] Offset commit failed on partition test-topic-2 at offset 300: This server does not host this topic-partition. (org.apache.kafka.clients.consumer.internals.ConsumerCoordinator)
connect-1             | [2019-06-03 06:56:17,731] WARN [Consumer clientId=consumer-9, groupId=connect-s3-confluent-sink] Error while fetching metadata with correlation id 2189 : {test-topic=UNKNOWN_TOPIC_OR_PARTITION} (org.apache.kafka.clients.NetworkClient)
connect-1             | [2019-06-03 06:56:17,732] ERROR [Consumer clientId=consumer-9, groupId=connect-s3-confluent-sink] Offset commit failed on partition test-topic-2 at offset 300: This server does not host this topic-partition. (org.apache.kafka.clients.consumer.internals.ConsumerCoordinator)
connect-1             | [2019-06-03 06:56:17,833] WARN [Consumer clientId=consumer-9, groupId=connect-s3-confluent-sink] Error while fetching metadata with correlation id 2191 : {test-topic=UNKNOWN_TOPIC_OR_PARTITION} (org.apache.kafka.clients.NetworkClient)
connect-1             | [2019-06-03 06:56:17,834] ERROR [Consumer clientId=consumer-9, groupId=connect-s3-confluent-sink] Offset commit failed on partition test-topic-2 at offset 300: This server does not host this topic-partition. (org.apache.kafka.clients.consumer.internals.ConsumerCoordinator)
connect-1             | [2019-06-03 06:56:17,935] WARN [Consumer clientId=consumer-9, groupId=connect-s3-confluent-sink] Error while fetching metadata with correlation id 2193 : {test-topic=UNKNOWN_TOPIC_OR_PARTITION} (org.apache.kafka.clients.NetworkClient)
connect-1             | [2019-06-03 06:56:17,935] ERROR [Consumer clientId=consumer-9, groupId=connect-s3-confluent-sink] Offset commit failed on partition test-topic-2 at offset 300: This server does not host this topic-partition. (org.apache.kafka.clients.consumer.internals.ConsumerCoordinator)
```