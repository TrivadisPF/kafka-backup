## Backup of Compacted Log Topics

The idea of the backup of compacted log topics is that a backup is always starting from offset 0 and backups all the data into S3. Conceptually we will use an **Active/Passive** setup. There are always to identical instances ready for doing a backup, one in **Active** and one in **Passive** mode. In order to keep the size of the backup small, after some time, the active backup instance informs the one in passive state that is should take over the work. The passive instance will start its backup (again from offset 0) and as soon as it reaches the end of the topic, it will switch to **active state** and inform the previous active one to change itself to passive state. Upon changing to passive state, the instance will remove its backup data on S3.  

The following diagram shows the two backup instances running for a Log Compacted Topics. 

![Alt Image Text](./images/backup-log-compacted.png "Handling Log Compacted topics")

The following steps describe one cycle of the backup and the switch from **Active** to **Passive**

 1. one instance starts in **Active** (white background) 
 2. and one instance starts in **Passive** state (grey background)
 3. the active instance starts its backup from offset 0 
 4. the backup runs until a configurable time has passed (`compacted.log.backup.length.days`)
 5. the active instance publishes a message to topic `_compacted_log_backup_coordination` according to this [Avro Schema](../src/main/avro/AvroCompatedLogBackupCoordination-v1.0.avsc)
 6. the Passive instance picks up the message and changes its state to **Catch-up**
 7. clears its backup destination on S3 
 8. and (re)starts its own backup from offset 0
 9. if the passive instance is at the end of the topic, it sends another message to `_compacted_log_backup_coordination`, this time to inform the Active instance to switch into **Passive** state
 10. The Active instance consumes the message 
 11. clear its backup destination on S3
 12. and changes its state to **Passive**
 13. the Passive instances changes its state to **Active** while continuing backing up the data

Now the circle restarts with the roles (**Active** and **Passive**) switched.

The determination of the `compacted.log.backup.length.days` has to be done use-case specific. It will differ depending on the throughput on the Topic as well as how many updates per key we get during a certain timeframe. 

The following diagram shows the usage of the coordination topic in more detail. 

![Alt Image Text](./images/backup-log-compacted-coord.png "Handling Log Compacted topics")


In order to keep the solution simple at the beginning, we could always start twpo instances of backup with identical configuration, except that one would start-up in **Passive** and one in **Active** state. 

Option questions and possible solutions

  1. What does **Active** and **Passive** mode mean in the context of a Kafka Connector?
    
     * we could use the **Pause** and **Resume** commands of a connector to match **Passive** and **Active** modes. A connector which is paused will no longer receive messages from the Connector. There are two corresponding methods `pause` and `resume` on the `SinkTaskContext` class (an instance of this class being available inside the connector)	
  2. How to start a backup from the beginning, if a connector was already running
    
     * use the [`offset`](http://raovat2.champhay.com/apache/kafka/2.2.1/javadoc/org/apache/kafka/connect/sink/SinkTaskContext.html#offset-java.util.Map) method on the `SinkTaskContext` class before switching the connector back to **Active**

  3. Is a connector in **Passive** state able to consume a message from Kafka, assuming that the connector instance has been **Paused**?
    
     * if this is not possible, then an option would be to implement a special Kafka Source Connector, which just reads the topic  `_compacted_log_backup_coordination` as well and executes the **Pause** and **Resume** command accordingly.

 		![Alt Image Text](./images/backup-log-compacted-state-coord.png "Handling Log Compacted topics")

  4. How do we know inside the **Passive** instance that we are at the end of the Topic, when we are "catching-up" so that we can inform the **Active** instance to **PASSIVATE**?
    
     * one option would be to also consume from the `__consumer_offsets` topic and filter for the connector consumer instance which is currently active. This is shown in the diagram above as well. This solution although is quite resource intensive!
     * the better option might be to use the Kafka `AdminClient` and the method [`listConsumerGroups`](https://kafka.apache.org/20/javadoc/org/apache/kafka/clients/admin/AdminClient.html#listConsumerGroupOffsets-java.lang.String-) to get a list of offsets for the given consumer group. 
     * another option to wait until the timestamp is "near" the now is not feasible, if we have topics with rather low-volume messaging and we haven't gotten a new message for a long time
     * if we get the offset until where to catch-up from the **Active** instance with the **ACTIVATE** instance, then we will only catch-up until this offset, while the **Active** backup has already progressed (assuming that there is traffic on the topic). See also 5).  

  5. Should we send the offset instead of trying to find out the end of the topic (see 4) ?   
     * we could expand the **ACTIVATE** message ([Avro Schema](../src/main/avro/AvroCompatedLogBackupCoordination-v1.0.avsc)) to pass the offset as well as the partition number, once the time is off and a switch should happen. The Kafka connect backup task would have to cache the latest offset per partition, otherwise there is no point of knowing the offset for a partition, if the current message set received in the Task does not contain any message for that partition.      
     * if we pass the current offset and partition of the **Active** instance (how far did it backup) in the **ACTIVATE** message then we can catch-up until this offset and then inform the still **Active** instance to stop it's backup by sending the **PASSIVATE** message. The problem with that solution is, that with a high-volume topic, we will stop the **Active** instance before we have really caught-up with the **Passive** instance, which means that there is a potential data loss between the time of stopping the **Active** and the new **Active** arriving at the end of the topic (and his backup is really up-to-date).
 
