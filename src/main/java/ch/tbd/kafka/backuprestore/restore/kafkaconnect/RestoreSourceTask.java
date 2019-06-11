package ch.tbd.kafka.backuprestore.restore.kafkaconnect;

import ch.tbd.kafka.backuprestore.model.KafkaRecord;
import ch.tbd.kafka.backuprestore.restore.deserializers.KafkaRecordDeserializer;
import ch.tbd.kafka.backuprestore.restore.deserializers.avro.KafkaRecordAvroDeserializer;
import ch.tbd.kafka.backuprestore.restore.kafkaconnect.config.RestoreSourceConnectorConfig;
import ch.tbd.kafka.backuprestore.util.AmazonS3Utils;
import ch.tbd.kafka.backuprestore.util.Constants;
import ch.tbd.kafka.backuprestore.util.Version;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class RestoreSourceTask extends SourceTask {

    private final Logger logger = LoggerFactory.getLogger(RestoreSourceTask.class);
    private final static String SEPARATOR = "/";
    private KafkaRecordDeserializer kafkaRecordDeserializer = new KafkaRecordAvroDeserializer();

    private static final String TOPIC_PARTITION_FIELD = "TOPIC_PARTITION_NAME";
    private static final String TOPIC_POSITION_FIELD = "TOPIC_POSITION_FIELD";

    private RestoreSourceConnectorConfig connectorConfig;
    private AmazonS3 amazonS3;
    private int[] partitionAssigned = null;
    private Map<Integer, Long> lastOffsetCommittedOnKafka = new HashMap<>();
    private Map<Integer, Long> lastOffsetS3Read = new HashMap<>();
    private final AtomicBoolean running = new AtomicBoolean(false);

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
        if (this.amazonS3 == null) {
            this.amazonS3 = AmazonS3Utils.initConnection(connectorConfig.getRegionConfig(), connectorConfig.getProxyUrlConfig(), connectorConfig.getProxyPortConfig());
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
        for (int i = 0; i < partitionAssigned.length && hasMoreSpaceToAddRecords(sourceRecordList); i++) {
            ListObjectsV2Request objectsPartitionReq = new ListObjectsV2Request().withBucketName(connectorConfig.getBucketName()).
                    withPrefix(connectorConfig.getTopicS3Name() + SEPARATOR + partitionAssigned[i] + SEPARATOR);
            ListObjectsV2Result resultPartitionReq = amazonS3.listObjectsV2(objectsPartitionReq);
            List<S3ObjectSummary> s3ObjectSummaries = resultPartitionReq.getObjectSummaries();
            Collections.sort(s3ObjectSummaries, Comparator.comparing(S3ObjectSummary::getLastModified));
            s3ObjectSummaries.stream().forEach(s3ObjectSummary -> {
                GetObjectRequest getObjectRequest = new GetObjectRequest(connectorConfig.getBucketName(), s3ObjectSummary.getKey());
                LinkedList<KafkaRecord> kafkaRecordLinkedList = convertS3ObjectToKafkaRecords(amazonS3.getObject(getObjectRequest).getObjectContent());
                kafkaRecordLinkedList.stream().forEach(kafkaRecord -> {
                    if (hasMoreSpaceToAddRecords(sourceRecordList)
                            && checkValidOffsetOnKafka(kafkaRecord.getPartition(), kafkaRecord.getOffset())
                            && checkValidOffsetS3(kafkaRecord.getPartition(), kafkaRecord.getOffset())) {
                        Map<String, String> sourcePartition = Collections.singletonMap(TOPIC_PARTITION_FIELD, keyPartitionOffsetKafkaConnect(kafkaRecord.getPartition()));
                        Map<String, Long> sourceOffset = Collections.singletonMap(TOPIC_POSITION_FIELD, kafkaRecord.getOffset());
                        lastOffsetS3Read.put(kafkaRecord.getPartition(), kafkaRecord.getOffset());
                        sourceRecordList.add(new SourceRecord(sourcePartition, sourceOffset, connectorConfig.getTopicKafkaName(),
                                kafkaRecord.getPartition(), Schema.BYTES_SCHEMA, kafkaRecord.getKey().array(),
                                Schema.BYTES_SCHEMA, kafkaRecord.getValue().array(), kafkaRecord.getTimestamp(), headerList(kafkaRecord.getHeaders())));
                    }
                });
            });
        }
        return sourceRecordList;
    }

    @Override
    public void initialize(SourceTaskContext context) {
        super.initialize(context);
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        while (running.get()) {
            if (partitionAssigned == null || partitionAssigned.length == 0) {
                logger.error("Please check the configuration. No partition assigned to restore {}", partitionAssigned);
                return Collections.emptyList();
            }
            logger.info("poll {} {}", Thread.currentThread().getName(), partitionAssigned);
            //restore
            List<SourceRecord> list = restore();
            if (list.isEmpty()) {
                logger.info("No data to restore");
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
        logger.info("Stop Restore source task");
        running.set(false);
    }

    @Override
    public void commitRecord(SourceRecord record) throws InterruptedException {
        super.commitRecord(record);
        logger.info("CommitRecord method -> {} ", record.toString());
    }

    private List<Header> headerList(Map<String, ByteBuffer> mapHeaders) {
        ConnectHeaders connectHeaders = new ConnectHeaders();
        connectHeaders.addLong(Constants.KEY_HEADER_RESTORED, new Date().getTime());
        connectHeaders.addBoolean(Constants.KEY_HEADER_RECOVER, Boolean.valueOf(true));
        List<Header> headerList = new ArrayList<>();
        if (mapHeaders != null && !mapHeaders.isEmpty()) {
            mapHeaders.keySet().iterator().forEachRemaining(header -> {
                ByteBuffer value = mapHeaders.get(header);
                connectHeaders.add(header, value, Schema.STRING_SCHEMA);
            });
        }
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

    private String keyPartitionOffsetKafkaConnect(int partition) {
        return this.connectorConfig.getTopicS3Name() + "-" + this.connectorConfig.getTopicKafkaName() + "-" + partition;
    }

}
