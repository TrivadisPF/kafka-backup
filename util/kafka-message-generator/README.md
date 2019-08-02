# Kafka Message generator

KafkaBinaryProducer is a command line utility that you can use to produce a set of binary messages to a Kafka topic. You can specify the size of the message and the distribution over the partitions of the topic.

## Build

```
mvn package
```

## Configuration

You can either use a config file (`-f`) to specify the various options or pass them as different parameters to the CLI. 

Running the CLI without parameter or using the `-h` parameter shows the Usage help

```
usage: KafkaBinaryProducer
 -b,--bootstrapServers <arg>        Bootstrap broker(s) (host[:port])
 -c,--create                        Create Kafka topic before producing
                                    data
 -d,--partitionDistribution <arg>   Distribution over partitions, if not
                                    set then it is equally distributed.
                                    Otherwise pass the number of messages
                                    per partition as a list of integers
 -f,--confFile <arg>                read the config from the file
 -h,--help                          Print Usage help
 -n,--numberOfMessages <arg>        Number of messages to produce in total
                                    over all partitions
 -p,--partitions <arg>              Number of Partitions, only needed when
                                    topic should be created before running
                                    (-c). Defaults to 1.
 -r,--replicationFactor <arg>       Replication Factor, only needed when
                                    topic should be created before running
                                    (-c). Defaults to 1.
 -s,--messageSize <arg>             The size of the message to produce,
                                    use KB or MB to specify the unit
 -t,--topic <arg>                   Topic to write messages to
 -w,--watchDir <arg>                Folder to watch on local filesystem
                                    for new files to be used as data. Defaults to /tmp
```

The CLI can either use a topic which is already existing or by using the `-c` option you can let the CLI create the topic before producing the messages. 

The utility watches a folder (`--watchDir` or `-w`) for new files, and will read a new file and splits it into blocks sized according to the `-s` (message size) option. A file is only read once, so you need to create a new file, if additional data needs to be created. By using the `-n` (number of messages) option, you can specify the total number of messages to generate. 

If the `-n` option is not used, then the input file will be read once, and as many messages as the file contains are generated (i.e. number of messages = file size / message size).

If the `-n` option is used, an it is larger than the number of messages the file provides, then the same data is repeated (file is reread from the beginning). 

The CLI is counting the messages it produces to each single partition of the topic and sends that count in the message header `message.number`. 

## Generate a large file with random content

In order to use the utility, a file is used to drive the production of the messages. It can be generated using the `dd` command. 

To create a file of size 100MB with random content, you can use the following command

```
dd if=/dev/urandom of=file.txt bs=1048576 count=100
```

Make sure that you place the file into the watch folder, specified when running the CLI.

## Examples

Create messages of size 10KB equally distributed over the partitions

```
java -jar target/kafka-message-generator-00.01.00.01-SNAPSHOT.jar -t=test-topic -b=analyticsplatform:9092 -s=10KB
```

You can use all the units for the message size (`-s`) supported by the [DataSize](https://docs.spring.io/spring/docs/current/javadoc-api/org/springframework/util/unit/DataSize.html) class of the Spring Framework. 

Create messages of size 10KB equally distributed over the partitions (assuming that the topic `test-topic` contains 8 partitions. By using the `-d` parameter, we tell the CLI to produce 4x more data to partition 0 and 2:

```
java -jar target/kafka-message-generator-00.01.00.01-SNAPSHOT.jar -t=test-topic -b=analyticsplatform:9092 -s=10KB -d="4,1,4,1,1,1,1,1"
```

To specify the number of messages to produce, you can use the `-n` option. 

```
java -jar target/kafka-message-generator-00.01.00.01-SNAPSHOT.jar -t=test-topic -b=analyticsplatform:9092 -s=10KB -n=10000
```


To create the topic before producing data, use the `-c` and optionally the `-p` and `-r` parameter to specify the partitions and the replication factor, which otherwise default to `1`.

```
java -jar target/kafka-message-generator-00.01.00.01-SNAPSHOT.jar -t=test-topic -b=analyticsplatform:9092 -s=10KB -c -p=8
```


