package ch.tbd.kafka.backuprestore.util;

import ch.tbd.kafka.backuprestore.common.kafkaconnect.AbstractBaseConnectorConfig;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class AmazonS3Utils.
 * This represents TODO.
 *
 * @author iorfinoa
 * @version $$Revision$$
 */
public class AmazonS3Utils {

    private static Logger logger = LoggerFactory.getLogger(AmazonS3Utils.class);

    public static AmazonS3 initConnection(AbstractBaseConnectorConfig connectorConfig) {
        AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();
        builder.withRegion(connectorConfig.getRegionConfig());
        builder.setCredentials(new ProfileCredentialsProvider());
        if (connectorConfig.getProxyUrlConfig() != null && !connectorConfig.getProxyUrlConfig().isEmpty() && connectorConfig.getProxyPortConfig() > 0) {
            ClientConfiguration config = new ClientConfiguration();
            config.setProtocol(Protocol.HTTPS);
            config.setProxyHost(connectorConfig.getProxyUrlConfig());
            config.setProxyPort(connectorConfig.getProxyPortConfig());
            builder.withClientConfiguration(config);
        }
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


}
