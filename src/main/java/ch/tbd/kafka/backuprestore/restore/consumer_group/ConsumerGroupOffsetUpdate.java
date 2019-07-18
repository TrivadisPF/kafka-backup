package ch.tbd.kafka.backuprestore.restore.consumer_group;

import ch.tbd.kafka.backuprestore.model.KafkaRecord;
import ch.tbd.kafka.backuprestore.util.AmazonS3Utils;
import ch.tbd.kafka.backuprestore.util.Constants;
import ch.tbd.kafka.backuprestore.util.ConsumerOffsetsUtils;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.commons.cli.*;
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
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.ByteBufferDeserializer;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.storage.SimpleHeaderConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static ch.tbd.kafka.backuprestore.common.kafkaconnect.AbstractBaseConnectorConfig.*;

/**
 * Class ConsumerGroupOffsetUpdate.
 * This represents TODO.
 *
 * @author iorfinoa
 * @version $$Revision$$
 */
public class ConsumerGroupOffsetUpdate {

    private static Logger logger = LoggerFactory.getLogger(ConsumerGroupOffsetUpdate.class);
    private static final String S3_TOPIC_NAME_KEY = "s3.topic.name";
    private Properties propertiesFile = new Properties();

    public static void main(String[] args) {

        final Options options = new Options();
        options.addOption(Option.builder("f").longOpt("file").hasArg().desc("Properties file which contains the data to open connection").required().build());
        options.addOption(Option.builder("t").longOpt("topics").hasArg().desc("Topic association old_topic:new_topic [new_topic is optional]").required().build());
        options.addOption(Option.builder("c").longOpt("consumergroup").hasArg().desc("Consumer group name to update").required().build());

        options.addOption(Option.builder("o").longOpt("offsets").hasArg().desc("Map old partition:offsets to update").build());

        options.addOption(Option.builder("s").longOpt("s3.file").hasArg().desc("Properties file which contains the parameter to open S3 connection").build());

        options.addOption(Option.builder("h").longOpt("help").hasArg(false).desc("Print Usage help").build());

        HelpFormatter formatter = new HelpFormatter();

        CommandLineParser parser = new DefaultParser();
        try {
            CommandLine cmd = parser.parse(options, args);
            if (cmd.getOptions().length == 0 || cmd.hasOption("h")) {
                formatter.printHelp("gen", options);
            } else {
                ConsumerGroupOffsetUpdate consumerGroupOffsetUpdate = new ConsumerGroupOffsetUpdate();
                consumerGroupOffsetUpdate.execute(cmd);
            }
        } catch (ParseException e) {
            logger.error(e.getMessage(), e);
        }
    }

    public void execute(CommandLine commandLine) {

        ConfigConsumerGroupOffsets configConsumerGroupOffsets = new ConfigConsumerGroupOffsets(commandLine);
        if (!configConsumerGroupOffsets.isPartitionOffsetDefined() && configConsumerGroupOffsets.isS3AmazonConfigurationDefined()) {
            extractOffsetFromS3(configConsumerGroupOffsets);
        } else if (!configConsumerGroupOffsets.isPartitionOffsetDefined()) {
            extractOffset(configConsumerGroupOffsets);
        }

        logger.info("Configuration for Old Topic >{}< - New Topic >{}< - Consumer group >{}<", configConsumerGroupOffsets.getOldTopicName(),
                configConsumerGroupOffsets.getNewTopicName(), configConsumerGroupOffsets.getConsumerGroupName());

        SimpleHeaderConverter shc = new SimpleHeaderConverter();
        if (!configConsumerGroupOffsets.getMapOldPartitionOffsets().isEmpty()) {
            try (Consumer consumer = new KafkaConsumer(createPropertiesConsumer(configConsumerGroupOffsets.getConsumerGroupName() + "_RESTORE"))) {
                consumer.subscribe(Collections.singletonList(configConsumerGroupOffsets.getNewTopicName()), new ConsumerRebalanceListener() {
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
                        if (!configConsumerGroupOffsets.getMapOldPartitionOffsets().containsKey(partition)) {
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
                                configConsumerGroupOffsets.getMapNewPartitionOffsets().put(partition, newOffset);
                            }
                        }
                        if (configConsumerGroupOffsets.getMapOldPartitionOffsets().containsKey(partition) && configConsumerGroupOffsets.getMapOldPartitionOffsets().get(partition) == oldOffset) {
                            updateOffsetConsumerGroup(configConsumerGroupOffsets.getNewTopicName(), configConsumerGroupOffsets.getConsumerGroupName(), partition, newOffset);
                            configConsumerGroupOffsets.getMapOldPartitionOffsets().remove(partition);
                        }
                    }
                }
                if (!configConsumerGroupOffsets.getMapOldPartitionOffsets().isEmpty()) {
                    Iterator<Integer> partitionsNotUpdated = configConsumerGroupOffsets.getMapNewPartitionOffsets().keySet().iterator();
                    while (partitionsNotUpdated.hasNext()) {
                        int partition = partitionsNotUpdated.next();
                        long newOffset = configConsumerGroupOffsets.getMapNewPartitionOffsets().get(partition);
                        logger.warn("No latest offset found on new topic. Set new offset with the latest record");
                        updateOffsetConsumerGroup(configConsumerGroupOffsets.getNewTopicName(), configConsumerGroupOffsets.getConsumerGroupName(), partition, newOffset);
                    }
                }
            } finally {
                try {
                    shc.close();
                } catch (IOException e) {
                    logger.error(e.getMessage(), e);
                }
            }
        } else {
            throw new IllegalStateException("No offset found to update. Please check the configuration");
        }
    }

    private void extractOffset(ConfigConsumerGroupOffsets configuration) {
        try (AdminClient adminClient = AdminClient.create(createPropertiesAdminClient())) {
            ListConsumerGroupsResult result = adminClient.listConsumerGroups();
            try {
                Collection<ConsumerGroupListing> collectionConsumerGroupListing = result.all().get();
                collectionConsumerGroupListing.stream().forEach(a -> {
                    if (a.groupId().equalsIgnoreCase(configuration.getConsumerGroupName())) {
                        ListConsumerGroupOffsetsResult offsetsResult = adminClient.listConsumerGroupOffsets(configuration.getConsumerGroupName());
                        try {
                            Map<TopicPartition, OffsetAndMetadata> mapTopicAndPartitions = offsetsResult.partitionsToOffsetAndMetadata().get();
                            mapTopicAndPartitions.keySet().stream().filter(topicPartition -> {
                                if (topicPartition.topic().equalsIgnoreCase(configuration.getOldTopicName())) {
                                    return true;
                                }
                                return false;
                            }).forEach(topicPartition -> {
                                configuration.getMapOldPartitionOffsets().put(topicPartition.partition(), mapTopicAndPartitions.get(topicPartition).offset());
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

    private void extractOffsetFromS3(ConfigConsumerGroupOffsets configuration) {

        String bucketName = configuration.getPropertiesS3Amazon().getProperty(S3_BUCKET_CONFIG);

        String profileName = null;
        if (configuration.getPropertiesS3Amazon().containsKey(S3_PROFILE_NAME_CONFIG)) {
            profileName = configuration.getPropertiesS3Amazon().getProperty(S3_PROFILE_NAME_CONFIG);
        }
        String regionConfig = configuration.getPropertiesS3Amazon().getProperty(S3_REGION_CONFIG);

        boolean wanModeConfig = false;
        if (configuration.getPropertiesS3Amazon().containsKey(WAN_MODE_CONFIG)) {
            wanModeConfig = Boolean.valueOf(configuration.getPropertiesS3Amazon().getProperty(WAN_MODE_CONFIG));
        }

        String proxyUrlConfig = null;
        if (configuration.getPropertiesS3Amazon().containsKey(S3_PROXY_URL_CONFIG)) {
            proxyUrlConfig = configuration.getPropertiesS3Amazon().getProperty(S3_PROXY_URL_CONFIG);
        }

        String proxyUser = null;
        if (configuration.getPropertiesS3Amazon().containsKey(S3_PROXY_USER_CONFIG)) {
            proxyUser = configuration.getPropertiesS3Amazon().getProperty(S3_PROXY_USER_CONFIG);
        }

        String proxyPassStr = null;
        if (configuration.getPropertiesS3Amazon().containsKey(S3_PROXY_PASS_CONFIG)) {
            proxyPassStr = configuration.getPropertiesS3Amazon().getProperty(S3_PROXY_PASS_CONFIG);
        }

        Password proxyPass = new Password(proxyPassStr);

        Integer s3RetryBackoffConfig = 200;
        if (configuration.getPropertiesS3Amazon().containsKey(S3_RETRY_BACKOFF_CONFIG)) {
            s3RetryBackoffConfig = Integer.valueOf(configuration.getPropertiesS3Amazon().getProperty(S3_RETRY_BACKOFF_CONFIG));
        }

        Integer s3PartRetriesConfig = 3;
        if (configuration.getPropertiesS3Amazon().containsKey(S3_PART_RETRIES_CONFIG)) {
            s3PartRetriesConfig = Integer.valueOf(configuration.getPropertiesS3Amazon().getProperty(S3_PART_RETRIES_CONFIG));
        }

        boolean headersUseExpectContinue = true;
        if (configuration.getPropertiesS3Amazon().containsKey(HEADERS_USE_EXPECT_CONTINUE_CONFIG)) {
            headersUseExpectContinue = Boolean.valueOf(configuration.getPropertiesS3Amazon().getProperty(HEADERS_USE_EXPECT_CONTINUE_CONFIG));
        }

        AmazonS3 amazonS3 = AmazonS3Utils.initConnection(profileName, regionConfig, wanModeConfig, proxyUrlConfig,
                proxyUser, proxyPass, s3RetryBackoffConfig, s3PartRetriesConfig, headersUseExpectContinue);

        if (!configuration.getPropertiesS3Amazon().containsKey(S3_TOPIC_NAME_KEY)) {
            throw new IllegalArgumentException("No topic name to read on S3 configured. Please check the configuration");
        }
        String consumerOffsetFolderName = configuration.getPropertiesS3Amazon().getProperty(S3_TOPIC_NAME_KEY);

        ListObjectsRequest objectsPartitionReq = new ListObjectsRequest().withBucketName(bucketName).
                withPrefix(consumerOffsetFolderName + Constants.S3_KEY_SEPARATOR);

        ObjectListing resultPartitionReq = amazonS3.listObjects(objectsPartitionReq);
        if (resultPartitionReq != null) {
            List<S3ObjectSummary> s3ObjectSummaries = resultPartitionReq.getObjectSummaries();
            while (resultPartitionReq.isTruncated()) {
                resultPartitionReq = amazonS3.listNextBatchOfObjects(resultPartitionReq);
                s3ObjectSummaries.addAll(resultPartitionReq.getObjectSummaries());
            }

            Collections.sort(s3ObjectSummaries, Comparator.comparing(S3ObjectSummary::getKey).thenComparing(S3ObjectSummary::getLastModified));
            Iterator<S3ObjectSummary> it = s3ObjectSummaries.iterator();

            while (it.hasNext()) {
                S3ObjectSummary s3ObjectSummary = it.next();
                GetObjectRequest getObjectRequest = new GetObjectRequest(bucketName, s3ObjectSummary.getKey());
                LinkedList<KafkaRecord> kafkaRecordLinkedList = AmazonS3Utils.convertS3ObjectToKafkaRecords(amazonS3.getObject(getObjectRequest).getObjectContent());


                for (KafkaRecord kafkaRecord : kafkaRecordLinkedList) {
                    KeyConsumerGroup keyConsumerGroup = ConsumerOffsetsUtils.readMessageKey(kafkaRecord.getKey());
                    if (keyConsumerGroup.getGroup().equals(configuration.getConsumerGroupName())
                            && keyConsumerGroup.validRecord()) {
                        logger.info("EQUALS");
                        ValueConsumerGroup valueConsumerGroup = ConsumerOffsetsUtils.readMessageValue(kafkaRecord.getValue());
                        if (configuration.getMapOldPartitionOffsets().containsKey(keyConsumerGroup.getPartition())
                                && configuration.getMapOldPartitionOffsets().get(keyConsumerGroup.getPartition()) < valueConsumerGroup.getOffset()) {
                            configuration.getMapOldPartitionOffsets().put(keyConsumerGroup.getPartition(), valueConsumerGroup.getOffset());
                        } else {
                            configuration.getMapOldPartitionOffsets().put(keyConsumerGroup.getPartition(), valueConsumerGroup.getOffset());
                        }
                    }
                }
            }
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
