package ch.tbd.kafka.backuprestore.restore.kafkaconnect;

import ch.tbd.kafka.backuprestore.model.KafkaRecord;
import ch.tbd.kafka.backuprestore.model.avro.AvroKafkaRecord;
import ch.tbd.kafka.backuprestore.restore.deserializers.KafkaRecordDeserializer;
import ch.tbd.kafka.backuprestore.restore.deserializers.avro.KafkaRecordAvroDeserializer;
import ch.tbd.kafka.backuprestore.restore.kafkaconnect.config.RestoreSourceConnectorConfig;
import ch.tbd.kafka.backuprestore.util.AmazonS3Utils;
import ch.tbd.kafka.backuprestore.util.Constants;
import ch.tbd.kafka.backuprestore.util.SerializationDataUtils;
import ch.tbd.kafka.backuprestore.util.Version;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class RestoreSourceTask extends SourceTask {

    private final Logger logger = LoggerFactory.getLogger(RestoreSourceTask.class);

    private static final String TOPIC_PARTITION_FIELD = "TOPIC_PARTITION_NAME";
    private static final String TOPIC_POSITION_FIELD = "TOPIC_POSITION_FIELD";

    private RestoreSourceConnectorConfig connectorConfig;
    private AmazonS3 amazonS3;
    private String s3TopicName;
    private String kafkaTopicName;
    private int[] partitionAssigned = null;
    private Map<Integer, Long> lastOffsetCommittedOnKafka = new HashMap<>();
    private Map<Integer, Long> lastOffsetS3Read = new HashMap<>();
    private final AtomicBoolean running = new AtomicBoolean(false);
    private Set<String> keyRestored = new HashSet<>();

    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public void start(Map<String, String> map) {
        this.kafkaTopicName = map.get(Constants.KEY_TOPIC_NAME_TASK);
        this.s3TopicName = map.get(Constants.KEY_S3_TOPIC_NAME_TASK);
        String partitionAssignedInConnector = map.get(Constants.KEY_PARTITION_TASK);

        if (partitionAssignedInConnector != null && partitionAssignedInConnector.indexOf(";") == -1) {
            //I have only one partition
            this.partitionAssigned = new int[1];
            this.partitionAssigned[0] = Integer.parseInt(partitionAssignedInConnector);
        } else if (partitionAssignedInConnector != null && partitionAssignedInConnector.indexOf(";") > -1) {
            String[] split = partitionAssignedInConnector.split(";");
            this.partitionAssigned = new int[split.length];
            for (int i = 0; i < split.length; i++) {
                this.partitionAssigned[i] = Integer.parseInt(split[i]);
            }
        } else {
            logger.error("No partition assigned by Task. Please check the configuration {}", partitionAssignedInConnector);
        }
        this.connectorConfig = new RestoreSourceConnectorConfig(map);
        if (this.amazonS3 == null) {
            this.amazonS3 = AmazonS3Utils.initConnection(this.connectorConfig);
        }

        for (int i = 0; i < this.partitionAssigned.length; i++) {
            Map<String, Object> data = context.offsetStorageReader().offset(Collections.singletonMap(TOPIC_PARTITION_FIELD, keyPartitionOffsetKafkaConnect(this.partitionAssigned[i])));
            if (data != null && !data.isEmpty()) {
                lastOffsetCommittedOnKafka.put(this.partitionAssigned[i], (long) data.get(TOPIC_POSITION_FIELD));
            }
        }
        this.running.set(true);
    }

    private boolean checkValidOffsetS3(int partition, long offset) {
        return !lastOffsetS3Read.containsKey(partition) ||
                (lastOffsetS3Read.containsKey(partition) && lastOffsetS3Read.get(partition) < offset);
    }

    private boolean checkValidOffsetOnKafka(int partition, long offset) {
        return !lastOffsetCommittedOnKafka.containsKey(partition) ||
                (lastOffsetCommittedOnKafka.containsKey(partition) && lastOffsetCommittedOnKafka.get(partition) < offset);
    }

    private boolean hasMoreSpaceToAddRecords(List<SourceRecord> sourceRecordList) {
        return sourceRecordList.size() < connectorConfig.getBatchMaxRecordsConfig();
    }

    private List<SourceRecord> restore() {
        List<SourceRecord> sourceRecordList = new ArrayList<>();
        for (int i = 0; i < partitionAssigned.length; i++) {
            if (!hasMoreSpaceToAddRecords(sourceRecordList)) {
                break;
            }
            ListObjectsRequest objectsPartitionReq = null;
            if (this.connectorConfig.isInstanceNameToRestoreConfigDefined()) {
                objectsPartitionReq = new ListObjectsRequest().withBucketName(connectorConfig.getBucketName()).
                        withPrefix(s3TopicName + Constants.S3_KEY_SEPARATOR + this.connectorConfig.getInstanceNameToRestoreConfig() + Constants.S3_KEY_SEPARATOR + partitionAssigned[i] + Constants.S3_KEY_SEPARATOR);
            } else {
                objectsPartitionReq = new ListObjectsRequest().withBucketName(connectorConfig.getBucketName()).
                        withPrefix(s3TopicName + Constants.S3_KEY_SEPARATOR + partitionAssigned[i] + Constants.S3_KEY_SEPARATOR);
            }
            ObjectListing resultPartitionReq = amazonS3.listObjects(objectsPartitionReq);
            if (resultPartitionReq != null) {
                List<S3ObjectSummary> s3ObjectSummaries = resultPartitionReq.getObjectSummaries();
                while (resultPartitionReq.isTruncated()) {
                    resultPartitionReq = amazonS3.listNextBatchOfObjects(resultPartitionReq);
                    s3ObjectSummaries.addAll(resultPartitionReq.getObjectSummaries());
                }
                Collections.sort(s3ObjectSummaries, Comparator.comparing(S3ObjectSummary::getKey));
                Iterator<S3ObjectSummary> it = s3ObjectSummaries.iterator();
                while (it.hasNext()) {
                    if (!hasMoreSpaceToAddRecords(sourceRecordList)) {
                        break;
                    }
                    S3ObjectSummary s3ObjectSummary = it.next();
                    if (!keyRestored.contains(s3ObjectSummary.getKey())) {
                        GetObjectRequest getObjectRequest = new GetObjectRequest(connectorConfig.getBucketName(), s3ObjectSummary.getKey());
                        LinkedList<KafkaRecord> kafkaRecordLinkedList = AmazonS3Utils.convertS3ObjectToKafkaRecords(amazonS3.getObject(getObjectRequest).getObjectContent());
                        int countRecordsRemainToCommit = kafkaRecordLinkedList.size();
                        for (KafkaRecord kafkaRecord : kafkaRecordLinkedList) {
                            if (hasMoreSpaceToAddRecords(sourceRecordList)
                                    && checkValidOffsetOnKafka(kafkaRecord.getPartition(), kafkaRecord.getOffset())
                                    && checkValidOffsetS3(kafkaRecord.getPartition(), kafkaRecord.getOffset())) {
                                Map<String, String> sourcePartition = Collections.singletonMap(TOPIC_PARTITION_FIELD, keyPartitionOffsetKafkaConnect(kafkaRecord.getPartition()));
                                Map<String, Long> sourceOffset = Collections.singletonMap(TOPIC_POSITION_FIELD, kafkaRecord.getOffset());
                                lastOffsetS3Read.put(kafkaRecord.getPartition(), kafkaRecord.getOffset());
                                Object key = kafkaRecord.getKey().array().length == 0 ? null : kafkaRecord.getKey().array();
                                Object value = kafkaRecord.getValue().array().length == 0 ? null : kafkaRecord.getValue().array();
                                sourceRecordList.add(new SourceRecord(sourcePartition, sourceOffset, kafkaTopicName,
                                        kafkaRecord.getPartition(), Schema.BYTES_SCHEMA, key,
                                        Schema.BYTES_SCHEMA, value, kafkaRecord.getTimestamp(), headerList(kafkaRecord.getHeaders(), kafkaRecord.getOffset())));
                                countRecordsRemainToCommit--;
                            }
                        }
                        if (countRecordsRemainToCommit == 0) {
                            keyRestored.add(s3ObjectSummary.getKey());
                        }
                    }
                }
            }
        }
        return sourceRecordList;
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        while (running.get()) {
            if (partitionAssigned == null || partitionAssigned.length == 0) {
                logger.error("Please check the configuration. No partition assigned to restore {}", partitionAssigned);
                return Collections.emptyList();
            }
            //restore
            List<SourceRecord> list = restore();
            if (list.isEmpty()) {
                logger.info("No data to restore for partition {}", partitionAssigned);
                break;
            } else {
                logger.info("Send {} records to store on Kafka", list.size());
                return list;
            }
        }
        Thread.sleep(connectorConfig.getPollIntervalMsConfig());
        return Collections.emptyList();
    }

    @Override
    public void stop() {
        running.set(false);
    }

    private List<Header> headerList(Map<String, ByteBuffer> mapHeaders, long offset) {
        ConnectHeaders connectHeaders = new ConnectHeaders();
        connectHeaders.addString(Constants.KEY_HEADER_OLD_OFFSET, String.valueOf(offset));
        connectHeaders.addString(Constants.KEY_HEADER_RESTORED, String.valueOf(Calendar.getInstance().getTimeInMillis()));
        connectHeaders.addString(Constants.KEY_HEADER_RECOVER, String.valueOf(true));
        List<Header> headerList = new ArrayList<>();
        if (mapHeaders != null && !mapHeaders.isEmpty()) {
            mapHeaders.keySet().iterator().forEachRemaining(header -> {
                ByteBuffer value = mapHeaders.get(header);
                Object data = SerializationDataUtils.deserialize(value.array());
                connectHeaders.add(header, data, understandType(data));
            });
        }
        connectHeaders.iterator().forEachRemaining(header -> {
            headerList.add(header);
        });

        return headerList;
    }

    private Schema understandType(Object data) {
        if (data instanceof Integer) {
            return Schema.INT64_SCHEMA;
        } else if (data instanceof String) {
            return Schema.STRING_SCHEMA;
        } else if (data instanceof byte[]) {
            return Schema.BYTES_SCHEMA;
        } else if (data instanceof Float) {
            return Schema.FLOAT64_SCHEMA;
        } else if (data instanceof Boolean) {
            return Schema.BOOLEAN_SCHEMA;
        }
        return null;
    }



    private String keyPartitionOffsetKafkaConnect(int partition) {
        return s3TopicName + Constants.DASH_KEY_SEPARATOR + kafkaTopicName + Constants.DASH_KEY_SEPARATOR + partition;
    }

}
