package ch.tbd.kafka.backuprestore.restore.consumer_group;

import ch.tbd.kafka.backuprestore.util.Constants;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.ByteBufferDeserializer;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.storage.SimpleHeaderConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * Class ConsumerGroupOffsetUpdate.
 * This represents TODO.
 *
 * @author iorfinoa
 * @version $$Revision$$
 */
public class ConsumerGroupOffsetUpdate {

    private static Logger logger = LoggerFactory.getLogger(ConsumerGroupOffsetUpdate.class);
    private Properties propertiesFile = new Properties();

    public static void main(String[] args) {

        if (args.length < 3) {
            logger.error("Please insert the following data:");
            logger.error("   1 -> Properties file wich contains the data to open connection");
            logger.error("   2 -> TOPIC_NAME (old_topic_name:new_topic_name - new_topic_name is optional in case the topic have the same name)");
            logger.error("   3 -> CONSUMER_GROUP_NAME - Insert the consumer group to update");
            logger.error("   4 -> Map Partition-Offset (0:100) - This parameter is optional");
            System.exit(-1);
        }

        ConsumerGroupOffsetUpdate consumerGroupOffsetUpdate = new ConsumerGroupOffsetUpdate();
        consumerGroupOffsetUpdate.execute(args);
    }

    public void execute(String[] args) {
        FileInputStream file = null;
        try {
            file = new FileInputStream(args[0]);
            propertiesFile.load(file);
        } catch (FileNotFoundException e) {
            logger.error(e.getMessage(), e);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        } finally {
            if (file != null) {
                try {
                    file.close();
                } catch (IOException e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }

        String topic = args[1];
        String consumerGroupName = args[2];
        String oldTopic = null;
        String newTopic = null;
        if (topic.indexOf(":") > -1) {
            oldTopic = topic.split(":")[0];
            newTopic = topic.split(":")[1];
        } else {
            oldTopic = topic;
            newTopic = topic;
        }
        ConfigurationConsumerGroupOffset configuration =
                ConfigurationConsumerGroupOffset.createConfigurationConsumerGroupOffset(oldTopic, newTopic, consumerGroupName);
        logger.info("Configuration for Old Topic >{}< - New Topic >{}< - Consumer group >{}<", configuration.getOldTopicName(), configuration.getNewTopicName(), configuration.getConsumerGroup());

        if (args.length == 3) {
            extractOffset(configuration);
        } else if (args.length == 4) {
            setPartitionOffset(configuration, args[3]);
        } else {
            throw new IllegalArgumentException("Please configure correctly the input data");
        }


        SimpleHeaderConverter shc = new SimpleHeaderConverter();
        if (!configuration.getMapPartitionOldOffset().isEmpty()) {
            try (Consumer consumer = new KafkaConsumer(createPropertiesConsumer(configuration.getConsumerGroup() + "_RESTORE"))) {
                consumer.subscribe(Collections.singletonList(configuration.getNewTopicName()), new ConsumerRebalanceListener() {
                    @Override
                    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    }

                    @Override
                    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                        consumer.seekToBeginning(partitions);
                    }
                });
                int MAX_ATTEMPS = 5;
                int count = 0;
                boolean continueConsumeData = true;
                while (continueConsumeData) {
                    ConsumerRecords<ByteBuffer, ByteBuffer> consumerRecords = consumer.poll(Duration.ofMillis(1000));
                    if (consumerRecords.count() == 0) {
                        if (count < MAX_ATTEMPS) {
                            count++;
                            continueConsumeData = true;
                        } else {
                            continueConsumeData = false;
                        }
                        continue;
                    }
                    for (ConsumerRecord<ByteBuffer, ByteBuffer> record : consumerRecords) {
                        int partition = record.partition();
                        long newOffset = record.offset();
                        if (!configuration.getMapPartitionOldOffset().containsKey(partition)) {
                            continue;
                        }
                        Headers headers = record.headers();
                        long oldOffset = -1L;
                        for (Header header : headers.toArray()) {
                            String key = header.key();
                            byte[] value = header.value();
                            if (Constants.KEY_HEADER_OLD_OFFSET.equalsIgnoreCase(key)) {
                                SchemaAndValue schemaAndValue = shc.toConnectHeader(record.topic(), Constants.KEY_HEADER_OLD_OFFSET, value);
                                oldOffset = Long.valueOf(String.valueOf(schemaAndValue.value()));
                                configuration.getMapPartitionNewLatestOffset().put(partition, newOffset);
                            }
                        }
                        if (configuration.getMapPartitionOldOffset().containsKey(partition) && configuration.getMapPartitionOldOffset().get(partition) == oldOffset) {
                            updateOffsetConsumerGroup(configuration.getNewTopicName(), configuration.getConsumerGroup(), partition, newOffset);
                            configuration.getMapPartitionOldOffset().remove(partition);
                        }
                    }
                }
                if (!configuration.getMapPartitionOldOffset().isEmpty()) {
                    Iterator<Integer> partitionsNotUpdated = configuration.getMapPartitionNewLatestOffset().keySet().iterator();
                    while (partitionsNotUpdated.hasNext()) {
                        int partition = partitionsNotUpdated.next();
                        long newOffset = configuration.getMapPartitionNewLatestOffset().get(partition);
                        logger.warn("No latest offset found on new topic. Set new offset with the latest record");
                        updateOffsetConsumerGroup(configuration.getNewTopicName(), configuration.getConsumerGroup(), partition, newOffset);
                    }
                }
            } finally {
                try {
                    shc.close();
                } catch (IOException e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }
    }

    private void setPartitionOffset(ConfigurationConsumerGroupOffset configuration, String partitionOffset) {
        if (partitionOffset.indexOf(",") > -1) {
            String[] object = partitionOffset.split(",");
            for (String partitionOffsetString : object) {
                setOffset(configuration, partitionOffsetString);
            }
        } else {
            setOffset(configuration, partitionOffset);
        }
    }

    private void setOffset(ConfigurationConsumerGroupOffset configuration, String partitionOffsetString) {
        if (partitionOffsetString.indexOf(":") == -1) {
            throw new IllegalArgumentException("Partition-Offset data defined wrong");
        }
        String[] array = partitionOffsetString.split(":");
        Integer partition = Integer.parseInt(array[0]);
        Long offset = Long.valueOf(array[1]);
        configuration.getMapPartitionOldOffset().put(partition, offset);
    }

    private void extractOffset(ConfigurationConsumerGroupOffset configuration) {
        try (AdminClient adminClient = AdminClient.create(createPropertiesAdminClient())) {
            ListConsumerGroupsResult result = adminClient.listConsumerGroups();
            try {
                Collection<ConsumerGroupListing> collectionConsumerGroupListing = result.all().get();
                collectionConsumerGroupListing.stream().forEach(a -> {
                    if (a.groupId().equalsIgnoreCase(configuration.getConsumerGroup())) {
                        ListConsumerGroupOffsetsResult offsetsResult = adminClient.listConsumerGroupOffsets(configuration.getConsumerGroup());
                        try {
                            Map<TopicPartition, OffsetAndMetadata> mapTopicAndPartitions = offsetsResult.partitionsToOffsetAndMetadata().get();
                            mapTopicAndPartitions.keySet().stream().filter(topicPartition -> {
                                if (topicPartition.topic().equalsIgnoreCase(configuration.getOldTopicName())) {
                                    return true;
                                }
                                return false;
                            }).forEach(topicPartition -> {
                                configuration.getMapPartitionOldOffset().put(topicPartition.partition(), mapTopicAndPartitions.get(topicPartition).offset());
                            });
                        } catch (ExecutionException e) {
                            logger.error(e.getMessage(), e);

                        } catch (InterruptedException e) {
                            logger.error(e.getMessage(), e);
                        }
                    }
                });
            } catch (ExecutionException e) {
                logger.error(e.getMessage(), e);
            } catch (InterruptedException e) {
                logger.error(e.getMessage(), e);
            }
        } catch (KafkaException e) {
            logger.error(e.getMessage(), e);
        }
    }

    private void updateOffsetConsumerGroup(String topicName, String consumerGroup, int partition, long offset) {
        try (Consumer oldConsumer = new KafkaConsumer(createPropertiesConsumer(consumerGroup))) {
            oldConsumer.assign(Collections.singletonList(new TopicPartition(topicName, partition)));
            oldConsumer.seek(new TopicPartition(topicName, partition), offset);
            oldConsumer.commitSync();
            logger.info("Updated offset for topic {} - partition {} - newOffset {}", topicName, partition, offset);
        } catch (KafkaException e) {
            logger.error(e.getMessage(), e);
        }
    }

    private Properties createPropertiesAdminClient() {
        Properties properties = commonProperties();
        //properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        return properties;
    }

    private Properties createPropertiesConsumer(String consumerGroupId) {
        Properties properties = commonProperties();
        //properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        //properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteBufferDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteBufferDeserializer.class.getName());
        return properties;
    }

    private Properties commonProperties() {
        Properties properties = new Properties();
        properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, propertiesFile.getProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG));
        if (propertiesFile.containsKey(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG)) {
            properties.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, propertiesFile.getProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
        }
        if (propertiesFile.containsKey(SaslConfigs.SASL_MECHANISM)) {
            properties.setProperty(SaslConfigs.SASL_MECHANISM, propertiesFile.getProperty(SaslConfigs.SASL_MECHANISM));
        }
        properties.setProperty(CommonClientConfigs.CLIENT_ID_CONFIG, propertiesFile.getProperty(CommonClientConfigs.CLIENT_ID_CONFIG));

        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        if (propertiesFile.containsKey(SaslConfigs.SASL_JAAS_CONFIG)) {
            properties.setProperty(SaslConfigs.SASL_JAAS_CONFIG, propertiesFile.getProperty(SaslConfigs.SASL_JAAS_CONFIG));
        }

        if (propertiesFile.containsKey(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG)) {
            properties.setProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, propertiesFile.getProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG));
        }

        if (propertiesFile.containsKey(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG)) {
            properties.setProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, propertiesFile.getProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG));
        }
        return properties;
    }
}
