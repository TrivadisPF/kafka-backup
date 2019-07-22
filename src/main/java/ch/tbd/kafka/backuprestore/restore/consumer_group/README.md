
# Utility to reset the offset
This utility allow, after the restore process, to update the offset for specific consumer group.

There are three different method to reset the offset:
 - 1 Manually reset
 - 2 Reset offsets reading the offset inside the broker
 - 3 Reset offsets reading the information from the previous backup on S3

## Manually reset
The manual reset mechanism allow the possibility to define manually the new offset of the consumergroup on the topic restored.
An example how to execute this activity is the following

``
java -jar kafka-backup-restore.jar -f /consumer-group-offset-update.properties -t test-topic -c consumer_group_name -o 0:10,1:100,...
``

"-f" parameter mean the properties file which contains the parameter to open a connection to the broker (See at the end an example)

"-t" parameter mean the topic name to update. It is possible to use the syntax "old_topic_name:new_topic_name". In this case the name of the old topic name is not necessary. It is used on the reset procedure number 2

"-c" parameter mean the Consumer group name to update.

"-o" parameter mean the association of partitions:offset to set

## Reset offsets reading the offset inside the broker
This step could be helpful in case accidentally someone have deleted the topic from the broker. In this case the broker 
itself contains the consumer group status and offset. After the restore could be reuse the old information stored in
the broker to get the old offsets consumed and to update it on the new topic (restored).
During this execution you need to configure an user which privilege as Admin.
An example of the command to use:
 
``
java -jar kafka-backup-restore.jar -f /consumer-group-offset-update.properties -t old_topic:new_topic -c consumer_group_name
``

"-f" parameter mean the properties file which contains the parameter to open a connection to the broker (See at the end an example)

"-t" parameter mean the topic name to update. In this case the name of the old topic name is mandatory, because the procedure need to know from which topic get the offsets.

"-c" parameter mean the Consumer group name to update.

## Reset offsets reading the information from the previous backup on S3
Using this backup system is possible to configure a SinkConnector in order to backup the __consumer_offsets topic.
This topic contains the information about all offsets and could be interesting to have a backup in case broker problems.
Also in this case after the restore need to update the offsets, using this procedure it is possible to get the old information
from S3 backup.
The authentication for S3 is managed using the credentials file under ~/.aws folders. Please ensure to have configured the access_key and secret_access_key

``
java -jar kafka-backup-restore.jar -f /consumer-group-offset-update.properties -t old_topic:new_topic -c consumer_group_name -s /consumer-group-offset-update-s3.properties
``

"-f" parameter mean the properties file which contains the parameter to open a connection to the broker (See at the end an example)

"-t" parameter mean the topic name to update. It is possible to use the syntax "old_topic_name:new_topic_name". In this case the name of the old topic name is not necessary. It is used on the reset procedure number 2

"-c" parameter mean the Consumer group name to update.

"-s" parameter mean the properties file which contains the parameter to open a connection to the S3 Storage(See at the end an example)


#### Properties file for the broker connection
`bootstrap.servers               -> Mandatory`

`client.id                      -> Mandatory`

`security.protocol               -> Optional`

`sasl.mechanism                  -> Optional`

`sasl.jaas.config                -> Optional`

`ssl.truststore.location         -> Optional`

`ssl.truststore.password         -> Optional`

#### Properties file for S3 connection
`s3.topic.name                   -> Mandatory`

`s3.bucket.name                  -> Mandatory`

`s3.region                       -> Mandatory`

`s3.profile.name                 -> Optional`

`s3.wan.mode                     -> Optional`

`s3.proxy.url                    -> Optional`

`s3.proxy.user                   -> Optional`

`s3.proxy.password               -> Optional`

`s3.retry.backoff.ms             -> Optional`

`s3.part.retries                 -> Optional`

`s3.http.send.expect.continue    -> Optional`


