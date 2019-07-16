package ch.tbd.kafka.backuprestore.util;

import ch.tbd.kafka.backuprestore.common.kafkaconnect.AbstractBaseConnectorConfig;
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
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private static ClientConfiguration newClientConfiguration(AbstractBaseConnectorConfig config) {
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

        return clientConfiguration;
    }

    private static RetryPolicy newFullJitterRetryPolicy(AbstractBaseConnectorConfig config) {

        PredefinedBackoffStrategies.FullJitterBackoffStrategy backoffStrategy =
                new PredefinedBackoffStrategies.FullJitterBackoffStrategy(
                        config.getLong(S3_RETRY_BACKOFF_CONFIG).intValue(),
                        S3_RETRY_MAX_BACKOFF_TIME_MS
                );

        RetryPolicy retryPolicy = new RetryPolicy(
                PredefinedRetryPolicies.DEFAULT_RETRY_CONDITION,
                backoffStrategy,
                config.getS3PartRetries(),
                false
        );
        return retryPolicy;
    }


}
