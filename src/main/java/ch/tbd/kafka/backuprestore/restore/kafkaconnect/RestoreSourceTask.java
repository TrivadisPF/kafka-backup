package ch.tbd.kafka.backuprestore.restore.kafkaconnect;

import ch.tbd.kafka.backuprestore.model.KafkaRecord;
import ch.tbd.kafka.backuprestore.restore.deserializers.KafkaRecordDeserializer;
import ch.tbd.kafka.backuprestore.restore.deserializers.avro.KafkaRecordAvroDeserializer;
import ch.tbd.kafka.backuprestore.restore.kafkaconnect.config.RestoreSourceConnectorConfig;
import ch.tbd.kafka.backuprestore.util.AmazonS3Utils;
import ch.tbd.kafka.backuprestore.util.Constants;
import ch.tbd.kafka.backuprestore.util.IndexObj;
import ch.tbd.kafka.backuprestore.util.Version;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.*;

public class RestoreSourceTask extends SourceTask {

    private final Logger logger = LoggerFactory.getLogger(RestoreSourceTask.class);
    private final static String SEPARATOR = "/";
    private KafkaRecordDeserializer kafkaRecordDeserializer = new KafkaRecordAvroDeserializer();

    private RestoreSourceConnectorConfig connectorConfig;
    private AmazonS3 amazonS3;
    private int[] partitionAssigned = null;
    private Map<Integer, Long> lastOffsetStoredOnS3 = new HashMap<>();
    private Map<Integer, Long> lastOffsetRestored = new HashMap<>();
    private long sleepMillisecond = -1;


    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public void start(Map<String, String> map) {
        String partitionAssigned = map.get(Constants.PARTITION_ASSIGNED_KEY);
        if (partitionAssigned != null && partitionAssigned.indexOf(";") == -1) {
            //I have only one partition
            this.partitionAssigned = new int[1];
            this.partitionAssigned[0] = Integer.parseInt(partitionAssigned);
        } else if (partitionAssigned != null && partitionAssigned.indexOf(";") > -1) {
            String[] split = partitionAssigned.split(";");
            this.partitionAssigned = new int[split.length];
            for (int i = 0; i < split.length; i++) {
                this.partitionAssigned[i] = Integer.parseInt(split[i]);
            }
        } else {
            logger.error("No partition assigned by Task. Please check the configuration {}", partitionAssigned);
        }
        this.connectorConfig = new RestoreSourceConnectorConfig(map);
        this.amazonS3 = AmazonS3Utils.initConnection(connectorConfig.getRegionConfig(), connectorConfig.getProxyUrlConfig(), connectorConfig.getProxyPortConfig());

        /*
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
        */
    }

    /*
        private synchronized void createLockFile() {

            ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(connectorConfig.getBucketName()).withPrefix(connectorConfig.getRestoreTopicName() + SEPARATOR);
            ListObjectsV2Result result = amazonS3.listObjectsV2(req);

            List<S3ObjectSummary> s3ObjectSummaries = result.getObjectSummaries();
            LinkedHashSet<Integer> partitions = new LinkedHashSet<>();

            boolean lockInserted = false;

            s3ObjectSummaries.stream().forEach(s3ObjectSummary -> {
                logger.info("Key object {}", s3ObjectSummary.getKey());
                String[] keys = s3ObjectSummary.getKey().split(SEPARATOR);
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
    */

    private void initializeIndexToRestore(int partition) {
        try {
            S3Object fullObject = amazonS3.getObject(new GetObjectRequest(connectorConfig.getBucketName(), connectorConfig.getTopicS3Name() + SEPARATOR + partition + SEPARATOR + Constants.INDEX_RESTORED_FILE_NAME));
            if (fullObject != null) {
                IndexObj indexObj = SerializationUtils.deserialize(fullObject.getObjectContent());
                lastOffsetStoredOnS3.put(partition, indexObj.getOffset());
            }
        } catch (AmazonS3Exception e) {
            if (e.getStatusCode() != 404) {
                throw e;
            } else {
                logger.info("Index file for partition {} not found", partition);
            }
        }
    }

    private void updateIndex(int partition, long offset) {
        if (checkValidOffsetS3(partition, offset)) {
            logger.info("Updating index for partition {} with offset {}", partition, offset);
            PutObjectRequest request = new PutObjectRequest(connectorConfig.getBucketName(),
                    connectorConfig.getTopicS3Name() + SEPARATOR + partition + SEPARATOR + Constants.INDEX_RESTORED_FILE_NAME,
                    new ByteArrayInputStream(SerializationUtils.serialize(new IndexObj(partition, offset))), new ObjectMetadata());
            amazonS3.putObject(request);
        }
    }

    /*
    private void updateIndex(int partition, long index) {

        //Metadata empty
        ObjectMetadata metadata = new ObjectMetadata();
        String indexFileContent = "INDEX->" + index;

        PutObjectRequest request = new PutObjectRequest(connectorConfig.getBucketName(),
                connectorConfig.getRestoreTopicName() + SEPARATOR + partition + SEPARATOR + INDEX_RESTORED_FILE_NAME, new ByteArrayInputStream(indexFileContent.getBytes()), metadata);
        amazonS3.putObject(request);
    }
    */

    private boolean checkValidOffsetS3(int partition, long offset) {
        return !lastOffsetStoredOnS3.containsKey(partition) ||
                (lastOffsetStoredOnS3.containsKey(partition) && lastOffsetStoredOnS3.get(partition) < offset);
    }


    @Override
    public void commit() throws InterruptedException {
        super.commit();
    }

    private List<SourceRecord> restore() {
        List<SourceRecord> sourceRecordList = new ArrayList<>();
        for (int i = 0; i < partitionAssigned.length; i++) {
            initializeIndexToRestore(partitionAssigned[i]);

            ListObjectsV2Request objectsPartitionReq = new ListObjectsV2Request().withBucketName(connectorConfig.getBucketName()).
                    withPrefix(connectorConfig.getTopicS3Name() + SEPARATOR + partitionAssigned[i] + SEPARATOR);
            ListObjectsV2Result resultPartitionReq = amazonS3.listObjectsV2(objectsPartitionReq);
            List<S3ObjectSummary> s3ObjectSummaries = resultPartitionReq.getObjectSummaries();
            Collections.sort(s3ObjectSummaries, Comparator.comparing(S3ObjectSummary::getLastModified).reversed());
            //TODO: Manage the offset already restored
            s3ObjectSummaries.stream().filter(s3ObjectSummary -> {
                if (s3ObjectSummary.getKey().endsWith(Constants.INDEX_RESTORED_FILE_NAME)) {
                    return false;
                }
                return true;
            }).forEach(s3ObjectSummary -> {
                GetObjectRequest getObjectRequest = new GetObjectRequest(connectorConfig.getBucketName(), s3ObjectSummary.getKey());
                LinkedList<KafkaRecord> kafkaRecordLinkedList = convertS3ObjectToKafkaRecords(amazonS3.getObject(getObjectRequest).getObjectContent());
                kafkaRecordLinkedList.stream().forEach(kafkaRecord -> {
                    if (checkValidOffsetS3(kafkaRecord.getPartition(), kafkaRecord.getOffset())) {
                        Map<String, String> sourcePartition = Collections.singletonMap("source-partition", String.valueOf(kafkaRecord.getPartition()));
                        Map<String, Long> sourceOffset = Collections.singletonMap("source-offset", kafkaRecord.getOffset());
                        //TODO:Configure name new topic restored
                        lastOffsetRestored.put(kafkaRecord.getPartition(), kafkaRecord.getOffset());
                        sourceRecordList.add(new SourceRecord(sourcePartition, sourceOffset, connectorConfig.getTopicS3Name(),
                                kafkaRecord.getPartition(), Schema.BYTES_SCHEMA, kafkaRecord.getKey().array(),
                                Schema.BYTES_SCHEMA, kafkaRecord.getValue().array(), kafkaRecord.getTimestamp(), headerList(kafkaRecord.getHeaders())));
                    }
                });
            });
        }

        if (!sourceRecordList.isEmpty()) {
            sleepMillisecond = 5000;
        }

        return sourceRecordList;

    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        if (partitionAssigned == null || partitionAssigned.length == 0) {
            logger.error("Please check the configuration. No partition assigned to restore {}", partitionAssigned);
            return Collections.emptyList();
        }
        logger.info("poll {} {}", Thread.currentThread().getName(), partitionAssigned);
        if (sleepMillisecond > -1) {
            logger.warn("The thread {} have restored all records. Put it on hold for {}", Thread.currentThread().getName(), sleepMillisecond);
            Thread.sleep(sleepMillisecond);
        }

        //restore
        return restore();
    }

    @Override
    public void stop() {

    }

    @Override
    public void commitRecord(SourceRecord record) throws InterruptedException {
        super.commitRecord(record);
        updateIndex(record.kafkaPartition(), (Long) record.sourceOffset().get("source-offset"));
        logger.info("CommitRecord method -> {} ", record.toString());
    }

    private List<Header> headerList(Map<String, ByteBuffer> mapHeaders) {
        if (mapHeaders == null) {
            return Collections.emptyList();
        }
        List<Header> headerList = new ArrayList<>();
        ConnectHeaders connectHeaders = new ConnectHeaders();
        mapHeaders.keySet().iterator().forEachRemaining(header -> {
            ByteBuffer value = mapHeaders.get(header);
            connectHeaders.add(header, value, Schema.STRING_SCHEMA);
        });
        connectHeaders.iterator().forEachRemaining(header -> {
            headerList.add(header);
        });


        return headerList;
    }

    private LinkedList<KafkaRecord> convertS3ObjectToKafkaRecords(S3ObjectInputStream s3ObjectInputStream) {
        LinkedList<KafkaRecord> kafkaRecordLinkedList = new LinkedList<>();
        BufferedReader reader = new BufferedReader(new InputStreamReader(s3ObjectInputStream));
        String line = null;
        try {
            while ((line = reader.readLine()) != null) {
                byte[] row = Base64.getDecoder().decode(line.getBytes());
                kafkaRecordLinkedList.add(kafkaRecordDeserializer.deserialize(ByteBuffer.wrap(row)));
            }
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
        return kafkaRecordLinkedList;
    }

}
