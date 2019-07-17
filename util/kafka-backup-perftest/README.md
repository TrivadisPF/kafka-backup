# Kafka Message generator

KafkaBinaryProducer is a command line utility that you can use to produce a set of binary messages to a Kafka topic. You can specify the size of the message and the distribution over the partitions of the topic.

You can either use a config file (`-f`) to specify the various options or pass them as different parameters to the CLI. 

Running the CLI without parameter or using the `-h` parameter shows the Usage help

```
usage: KafkaBinaryProducer
 -b,--bootstrapServers <arg>        Bootstrap broker(s) (host[:port])
 -c,--messageCount <arg>            Number of messages to produce in total
                                    over all partitions
 -d,--partitionDistribution <arg>   Distribution over partitions, if not
                                    set then it is equally distributed.
                                    Otherwise pass the number of messages
                                    per partition as a list of integers
 -f,--confFile <arg>                read the config from the file
 -h,--help                          Print Usage help
 -s,--messageSize <arg>             The size of the message to produce,
                                    use KB or MB to specify the unit
 -t,--topic <arg>                   Topic to write messages to
 -w,--watchDir <arg>                Folder to watch on local filesystem
                                    for new files to be used as data
```

The CLI is counting the messages it produces to each single partition of the topic and sends that count in the message header `message.number`. 

## Examples

Create messages of size 10KB equally distributed over the partitions

```
java -jar target/kafka-backup-perftest-00.01.00.01-SNAPSHOT.jar -t=test-topic -b=analyticsplatform:9092 -s=10KB
```

You can use all the units for the message size (`-s`) supported by the [DataSize](https://docs.spring.io/spring/docs/current/javadoc-api/org/springframework/util/unit/DataSize.html) class of the Spring Framework. 

Create messages of size 10KB equally distributed over the partitions (assuming that the topic `test-topic` contains 8 partitions. By using the `-d` parameter, we tell the CLI to produce 4x more data to partition 0 and 2:

```
java -jar target/kafka-backup-perftest-00.01.00.01-SNAPSHOT.jar -t=test-topic -b=analyticsplatform:9092 -s=10KB -d="4,1,4,1,1,1,1,1"
```


## Generate a large file with random content

To generate a file of size 100MB with random content, you can use the following command

```
dd if=/dev/urandom of=file.txt bs=1048576 count=100
```

This produces a file of size 400MB

```
dd if=/dev/urandom of=file.txt bs=1048576 count=400
```

