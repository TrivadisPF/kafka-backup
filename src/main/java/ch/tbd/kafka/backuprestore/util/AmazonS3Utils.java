package ch.tbd.kafka.backuprestore.util;

import ch.tbd.kafka.backuprestore.common.kafkaconnect.AbstractBaseConnectorConfig;
import ch.tbd.kafka.backuprestore.model.KafkaRecord;
import ch.tbd.kafka.backuprestore.model.avro.AvroKafkaRecord;
import ch.tbd.kafka.backuprestore.restore.deserializers.KafkaRecordDeserializer;
import ch.tbd.kafka.backuprestore.restore.deserializers.avro.KafkaRecordAvroDeserializer;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.PredefinedClientConfigurations;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.retry.PredefinedBackoffStrategies;
import com.amazonaws.retry.PredefinedRetryPolicies;
import com.amazonaws.retry.RetryPolicy;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.types.Password;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedList;

import static ch.tbd.kafka.backuprestore.common.kafkaconnect.AbstractBaseConnectorConfig.*;

/**
 * Class AmazonS3Utils.
 * This represents TODO.
 *
 * @author iorfinoa
 * @version $$Revision$$
 */
public class AmazonS3Utils {

    private static Logger logger = LoggerFactory.getLogger(AmazonS3Utils.class);
    private static final String VERSION_FORMAT = "APN/1.0 Confluent/1.0 KafkaS3Connector/%s";

    public static AmazonS3 initConnection(AbstractBaseConnectorConfig connectorConfig) {
        ClientConfiguration clientConfiguration = newClientConfiguration(connectorConfig);
        AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();
        builder.withAccelerateModeEnabled(connectorConfig.getBoolean(WAN_MODE_CONFIG));
        builder.withRegion(connectorConfig.getRegionConfig());
        if (null == connectorConfig.getS3ProfileNameConfig()) {
            builder.setCredentials(new ProfileCredentialsProvider());
        } else {
            builder.setCredentials(new ProfileCredentialsProvider(connectorConfig.getS3ProfileNameConfig()));
        }
        builder.withClientConfiguration(clientConfiguration);
        return builder.build();
    }

    public static AmazonS3 initConnection(String profileNameConfig, String regionConfig, boolean wanModeConfig, String proxyUrlConfig, String proxyUser, Password proxyPass,
                                          Integer s3RetryBackoffConfig, Integer s3PartRetries, boolean useExpectToContinue) {
        ClientConfiguration clientConfiguration = newClientConfiguration(proxyUrlConfig, proxyUser, proxyPass, s3RetryBackoffConfig, s3PartRetries, useExpectToContinue);
        AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();
        builder.withAccelerateModeEnabled(wanModeConfig);
        builder.withRegion(regionConfig);
        if (null == profileNameConfig) {
            builder.setCredentials(new ProfileCredentialsProvider());
        } else {
            builder.setCredentials(new ProfileCredentialsProvider(profileNameConfig));
        }
        builder.withClientConfiguration(clientConfiguration);
        return builder.build();
    }

    public static void cleanLastBackup(AmazonS3 amazonS3, String bucketName, String connectorName, TopicPartition tp) {
        ListObjectsRequest request = new ListObjectsRequest().withBucketName(bucketName).
                withPrefix(tp.topic() + Constants.S3_KEY_SEPARATOR + connectorName +
                        Constants.S3_KEY_SEPARATOR + tp.partition() + Constants.S3_KEY_SEPARATOR);
        cleanBackup(amazonS3, request);
    }

    public static void cleanLastBackup(AmazonS3 amazonS3, String bucketName, String connectorName, String topicName) {
        ListObjectsRequest request = new ListObjectsRequest().withBucketName(bucketName).
                withPrefix(topicName + Constants.S3_KEY_SEPARATOR + connectorName +
                        Constants.S3_KEY_SEPARATOR);
        cleanBackup(amazonS3, request);
    }

    private static void cleanBackup(AmazonS3 amazonS3, ListObjectsRequest request) {
        ObjectListing resultPartitionReq = amazonS3.listObjects(request);
        boolean continueExtractKeys = true;
        while (continueExtractKeys) {
            if (resultPartitionReq != null) {
                resultPartitionReq.getObjectSummaries().stream().forEach(s3ObjectSummary -> {
                    logger.info("Name {} ", s3ObjectSummary.getKey());
                    amazonS3.deleteObject(request.getBucketName(), s3ObjectSummary.getKey());
                });
                if (!resultPartitionReq.isTruncated()) {
                    continueExtractKeys = false;
                } else {
                    resultPartitionReq = amazonS3.listNextBatchOfObjects(resultPartitionReq);
                }
            }
        }
    }

    public static ClientConfiguration newClientConfiguration(AbstractBaseConnectorConfig config) {
        /*
String version = String.format(VERSION_FORMAT, Version.getVersion());
        ClientConfiguration clientConfiguration = PredefinedClientConfigurations.defaultConfig();
        clientConfiguration.withUserAgentPrefix(version)
                .withRetryPolicy(newFullJitterRetryPolicy(config));
        if (StringUtils.isNotBlank(config.getString(S3_PROXY_URL_CONFIG))) {
            S3ProxyConfig proxyConfig = new S3ProxyConfig(config);
            clientConfiguration.withProtocol(proxyConfig.protocol())
                    .withProxyHost(proxyConfig.host())
                    .withProxyPort(proxyConfig.port())
                    .withProxyUsername(proxyConfig.user())
                    .withProxyPassword(proxyConfig.pass());
        }
        clientConfiguration.withUseExpectContinue(config.useExpectContinue());
*/
        return newClientConfiguration(config.getString(S3_PROXY_URL_CONFIG), config.getString(S3_PROXY_USER_CONFIG), config.getPassword(S3_PROXY_PASS_CONFIG),
                config.getLong(S3_RETRY_BACKOFF_CONFIG).intValue(), config.getS3PartRetries(), config.useExpectContinue());
    }

    public static ClientConfiguration newClientConfiguration(String proxyUrlConfig, String proxyUser, Password proxyPass, Integer s3RetryBackoffConfig, Integer s3PartRetries, boolean useExpectToContinue) {
        String version = String.format(VERSION_FORMAT, Version.getVersion());

        ClientConfiguration clientConfiguration = PredefinedClientConfigurations.defaultConfig();
        clientConfiguration.withUserAgentPrefix(version)
                .withRetryPolicy(newFullJitterRetryPolicy(s3RetryBackoffConfig, s3PartRetries));
        if (StringUtils.isNotBlank(proxyUrlConfig)) {
            S3ProxyConfig proxyConfig = new S3ProxyConfig(proxyUrlConfig, proxyUser, proxyPass);
            clientConfiguration.withProtocol(proxyConfig.protocol())
                    .withProxyHost(proxyConfig.host())
                    .withProxyPort(proxyConfig.port())
                    .withProxyUsername(proxyConfig.user())
                    .withProxyPassword(proxyConfig.pass());
        }
        clientConfiguration.withUseExpectContinue(useExpectToContinue);

        return clientConfiguration;
    }

    private static RetryPolicy newFullJitterRetryPolicy(AbstractBaseConnectorConfig config) {
        return newFullJitterRetryPolicy(config.getLong(S3_RETRY_BACKOFF_CONFIG).intValue(), config.getS3PartRetries());
    }

    private static RetryPolicy newFullJitterRetryPolicy(Integer s3RetryBackoffConfig, Integer s3PartRetries) {
        PredefinedBackoffStrategies.FullJitterBackoffStrategy backoffStrategy =
                new PredefinedBackoffStrategies.FullJitterBackoffStrategy(
                        s3RetryBackoffConfig,
                        S3_RETRY_MAX_BACKOFF_TIME_MS
                );

        RetryPolicy retryPolicy = new RetryPolicy(
                PredefinedRetryPolicies.DEFAULT_RETRY_CONDITION,
                backoffStrategy,
                s3PartRetries,
                false
        );
        return retryPolicy;
    }


    public static LinkedList<KafkaRecord> convertS3ObjectToKafkaRecords(S3ObjectInputStream s3ObjectInputStream) {
        KafkaRecordDeserializer kafkaRecordDeserializer = new KafkaRecordAvroDeserializer();
        LinkedList<KafkaRecord> kafkaRecordLinkedList = new LinkedList<>();
        DatumReader<AvroKafkaRecord> reader = new GenericDatumReader<>(AvroKafkaRecord.getClassSchema());
        try (DataFileStream<AvroKafkaRecord> objectDataFileStream = new DataFileStream<>(s3ObjectInputStream, reader)) {

            while (objectDataFileStream.hasNext()) {
                AvroKafkaRecord record = new AvroKafkaRecord();
                objectDataFileStream.next(record);
                kafkaRecordLinkedList.add(kafkaRecordDeserializer.deserialize(record.toByteBuffer()));
            }

        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
        return kafkaRecordLinkedList;
    }


}
