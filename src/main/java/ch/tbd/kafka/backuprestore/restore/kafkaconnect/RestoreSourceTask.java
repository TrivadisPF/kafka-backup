package ch.tbd.kafka.backuprestore.restore.kafkaconnect;

import ch.tbd.kafka.backuprestore.model.avro.AvroKafkaRecord;
import ch.tbd.kafka.backuprestore.restore.kafkaconnect.config.RestoreSourceConnectorConfig;
import ch.tbd.kafka.backuprestore.util.Version;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.SerializationUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.*;

public class RestoreSourceTask extends SourceTask {

    private final Logger logger = LoggerFactory.getLogger(RestoreSourceTask.class);
    private final static String SEPARATOR = "/";
    private final String LOCK_FILE_NAME = "lock.json";
    private final String INDEX_RESTORED_FILE_NAME = "index-restoring-file.avro-messages";

    private RestoreSourceConnectorConfig connectorConfig;
    private AmazonS3 amazonS3;
    private int partitionAssigned = -1;

    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public void start(Map<String, String> map) {
        connectorConfig = new RestoreSourceConnectorConfig(map);
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

        this.amazonS3 = builder.build();


    }

    private synchronized void createLockFile() {

        ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(connectorConfig.getBucketName()).withPrefix(connectorConfig.getRestoreTopicName() + SEPARATOR);
        ListObjectsV2Result result = amazonS3.listObjectsV2(req);

        List<S3ObjectSummary> s3ObjectSummaries = result.getObjectSummaries();
        LinkedHashSet<Integer> partitions = new LinkedHashSet<>();

        boolean lockInserted = false;

        s3ObjectSummaries.stream().forEach(s3ObjectSummary -> {
            logger.info("Key object {}", s3ObjectSummary.getKey());
            String[] keys = s3ObjectSummary.getKey().split("/");
            int partition = Integer.parseInt(keys[keys.length - 2]);
            partitions.add(partition);
        });

        logger.info("Found {} partitions", partitions.size());

        partitions.stream().forEach(partition -> {
            if (partitionAssigned < 0) {
                ListObjectsV2Request partitionReq = new ListObjectsV2Request().withBucketName(connectorConfig.getBucketName()).withPrefix(connectorConfig.getRestoreTopicName() + SEPARATOR + partition);
                ListObjectsV2Result resultPartitionReq = amazonS3.listObjectsV2(partitionReq);
                List<S3ObjectSummary> s3ObjectPartitionSummaries = resultPartitionReq.getObjectSummaries();
                logger.info("Partition {} - Found {} objects ", partition, s3ObjectPartitionSummaries.size());

                boolean lockFoundPartition = s3ObjectPartitionSummaries.stream().filter(s3ObjectPartitionSummary -> {
                    String[] keys = s3ObjectPartitionSummary.getKey().split("/");
                    String nameFile = keys[keys.length - 1];
                    logger.info("Name object {}", nameFile);
                    if (nameFile.equalsIgnoreCase(LOCK_FILE_NAME)) {
                        return true;
                    }
                    return false;
                }).count() > 0;

                if (!lockFoundPartition) {
                    partitionAssigned = partition;
                    String lockFileContent = "Partition locked during the restore";
                    //Metadata empty
                    ObjectMetadata metadata = new ObjectMetadata();
                    PutObjectRequest request = new PutObjectRequest(connectorConfig.getBucketName(),
                            connectorConfig.getRestoreTopicName() + SEPARATOR + partitionAssigned + SEPARATOR + LOCK_FILE_NAME, new ByteArrayInputStream(lockFileContent.getBytes()), metadata);
                    amazonS3.putObject(request);
                }
            }
        });
    }

    private void updateIndex(int index) {

        //Metadata empty
        ObjectMetadata metadata = new ObjectMetadata();
        String indexFileContent = "Arrivato a " + index;

        PutObjectRequest request = new PutObjectRequest(connectorConfig.getBucketName(),
                connectorConfig.getRestoreTopicName() + SEPARATOR + partitionAssigned + SEPARATOR + INDEX_RESTORED_FILE_NAME, new ByteArrayInputStream(indexFileContent.getBytes()), metadata);
        amazonS3.putObject(request);
    }

    private synchronized void deleteLockFile() {
        if (partitionAssigned > -1) {
            // do something

            DeleteObjectRequest deleteObjectRequest = new DeleteObjectRequest(connectorConfig.getBucketName(), connectorConfig.getRestoreTopicName() + SEPARATOR + partitionAssigned + SEPARATOR + LOCK_FILE_NAME);
            amazonS3.deleteObject(deleteObjectRequest);

        }
    }

    private void restore() {
        ListObjectsV2Request objectsPartitionReq = new ListObjectsV2Request().withBucketName(connectorConfig.getBucketName()).withPrefix(connectorConfig.getRestoreTopicName() + SEPARATOR + partitionAssigned + SEPARATOR);
        ListObjectsV2Result resultPartitionReq = amazonS3.listObjectsV2(objectsPartitionReq);
        List<S3ObjectSummary> s3ObjectSummaries = resultPartitionReq.getObjectSummaries();
        s3ObjectSummaries.stream().filter(s3ObjectSummary -> {
            if (s3ObjectSummary.getKey().endsWith(LOCK_FILE_NAME)) {
                return false;
            }
            return true;
        }).forEach(s3ObjectSummary -> {
            GetObjectRequest getObjectRequest = new GetObjectRequest(connectorConfig.getBucketName(), s3ObjectSummary.getKey());
            try {
                AvroKafkaRecord[] object = toObject(AvroKafkaRecord[].class, IOUtils.toByteArray(amazonS3.getObject(getObjectRequest).getObjectContent()));
                if (object != null) {
                    for (AvroKafkaRecord avroKafkaRecord : object) {
                        // TODO: Fix the schema. Could be possible to store a Json?

                /*
                SourceRecord sourceRecord = new SourceRecord(partitionAssigned, avroKafkaRecord.getOffset(), connectorConfig.getRestoreTopicName(), avroKafkaRecord.get avroKafkaRecord.getSchema(), value);

                 */
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        logger.info("poll");
        List<SourceRecord> recordsToStore = new ArrayList<>();
        try {
            //create lock file
            createLockFile();

            if (partitionAssigned < 0) {
                logger.info("No partition available to restore for topic {} ", connectorConfig.getRestoreTopicName());
                return Collections.emptyList();
            }

            //restore
            restore();


        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            //remove lock file
            deleteLockFile();
        }


        return recordsToStore;
    }

    @Override
    public void stop() {

    }


    public static <T> T toObject(Class<T> clazz, byte[] bytes) {
        ByteArrayInputStream bis = null;
        ObjectInputStream ois = null;
        try {
            bis = new ByteArrayInputStream(bytes);
            ois = new ObjectInputStream(bis);
            return clazz.cast(ois.readObject());
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            if (bis != null) {
                try {
                    bis.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (ois != null) {
                try {
                    ois.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return null;
    }
}
