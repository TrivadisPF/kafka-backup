package ch.tbd.kafka.backuprestore.backup.kafkaconnect.compact;

import ch.tbd.kafka.backuprestore.backup.kafkaconnect.BackupSinkTask;
import ch.tbd.kafka.backuprestore.backup.kafkaconnect.compact.config.CompactBackupSinkConnectorConfig;
import ch.tbd.kafka.backuprestore.backup.storage.partitioner.DefaultPartitioner;
import ch.tbd.kafka.backuprestore.backup.storage.partitioner.Partitioner;
import ch.tbd.kafka.backuprestore.backup.storage.partitioner.TopicPartitionWriter;
import ch.tbd.kafka.backuprestore.model.avro.AvroCompactedLogBackupCoordination;
import ch.tbd.kafka.backuprestore.model.avro.EnumType;
import ch.tbd.kafka.backuprestore.util.AmazonS3Utils;
import ch.tbd.kafka.backuprestore.util.Version;
import com.amazonaws.services.s3.AmazonS3;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.ByteBufferDeserializer;
import org.apache.kafka.common.serialization.ByteBufferSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.MessageFormat;
import java.time.Duration;
import java.util.*;

/**
 * Class CompactBackupSinkTask.
 * This represents TODO.
 *
 * @author iorfinoa
 * @version $$Revision$$
 */
public class CompactBackupSinkTask extends SinkTask {


    private static final Logger logger = LoggerFactory.getLogger(BackupSinkTask.class);
    private CompactBackupSinkConnectorConfig connectorConfig;
    private final Set<TopicPartition> assignment;
    private final Map<TopicPartition, TopicPartitionWriter> topicPartitionWriters;
    private final Time time;
    private Partitioner<?> partitioner;
    private Consumer<String, ByteBuffer> kafkaConsumer;
    private Producer<String, ByteBuffer> kafkaProducer;
    private Calendar nextStart;
    private Calendar nextCheckToPassivateStatus;
    private Calendar nextCheckToActivateStatus;

    private final String TOPIC_COORDINATOR_NAME = "_compacted_log_backup_coordination";
    private AmazonS3 amazonS3;

    private Map<TopicPartition, Long> mapCoordinationTopic = new HashMap<>();
    private Map<TopicPartition, AvroCompactedLogBackupCoordination> mapActivatePartitions = new HashMap<>();
    private Set<AvroCompactedLogBackupCoordination> mapPassivatePartitions = new HashSet<>();
    private final int CHECK_COORDINATION_TOPIC_INTERVAL_IN_SEC = 30;

    private StatusConnector status = null;

    private enum StatusConnector {
        RUNNING, WAITING, ON_STARTING
    }

    /**
     * No-arg constructor. Used by Connect framework.
     */
    public CompactBackupSinkTask() {
        // no-arg constructor required by Connect framework.
        assignment = new HashSet<>();
        topicPartitionWriters = new HashMap<>();
        time = new SystemTime();
    }

    // visible for testing.
    CompactBackupSinkTask(CompactBackupSinkConnectorConfig connectorConfig, SinkTaskContext context,
                          Time time, Partitioner<?> partitioner) {
        this.assignment = new HashSet<>();
        this.topicPartitionWriters = new HashMap<>();
        this.connectorConfig = connectorConfig;
        this.context = context;
        this.partitioner = partitioner;
        this.time = time;
        open(context.assignment());
    }

    @Override
    public void start(Map<String, String> props) {
        this.connectorConfig = new CompactBackupSinkConnectorConfig(props);
        partitioner = new DefaultPartitioner<>();
        if (this.connectorConfig.getCompactedLogBackupInitialStatusConfig().equals(EnumType.ACTIVATE)) {
            status = StatusConnector.RUNNING;
        } else {
            status = StatusConnector.WAITING;
        }
        if (this.nextStart == null) {
            this.setNextDate();
        }
        if (this.amazonS3 == null) {
            this.amazonS3 = AmazonS3Utils.initConnection(this.connectorConfig);
        }
        if (this.kafkaProducer == null) {
            this.kafkaProducer = createProducer();
        }
    }

    @Override
    public void open(Collection<TopicPartition> partitions) {
        this.assignment.addAll(partitions);
        if (this.status.equals(StatusConnector.RUNNING) || this.status.equals(StatusConnector.ON_STARTING)) {
            initializeData();
        }
    }

    private void initializeData() {
        for (TopicPartition tp : this.assignment) {
            TopicPartitionWriter writer = new TopicPartitionWriter(
                    tp, this.connectorConfig, this.context, this.partitioner, this.time
            );
            this.topicPartitionWriters.put(tp, writer);
        }
    }

    private Calendar addTimeToWait(Calendar calendar) {
        if (calendar == null) {
            calendar = Calendar.getInstance();
        }
        calendar.add(Calendar.SECOND, CHECK_COORDINATION_TOPIC_INTERVAL_IN_SEC);
        return calendar;
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        for (SinkRecord record : records) {
            String topic = record.topic();
            int partition = record.kafkaPartition();
            long offset = record.kafkaOffset();
            TopicPartition tp = new TopicPartition(topic, partition);

            switch (this.status) {
                case WAITING:
                    logger.info("WAITING {} - Topic {} Partition {} Offset {}", this.connectorConfig.getName(), topic, partition, offset);
                    //if (mapActivatePartitions.isEmpty() || mapActivatePartitions.keySet().size() < this.assignment.size()) {
                    if (mapActivatePartitions.isEmpty() || !mapActivatePartitions.containsKey(tp)) {
                        if (mapActivatePartitions.isEmpty() && (nextCheckToActivateStatus == null || Calendar.getInstance().after(nextCheckToActivateStatus))) {
                            AvroCompactedLogBackupCoordination data = searchData(tp.topic(), tp.partition(), EnumType.ACTIVATE);
                            if (data != null) {
                                mapActivatePartitions.put(tp, data);
                                storeTumbstoneDataCoordinateTopic(tp.partition());
                            }
                            nextCheckToActivateStatus = addTimeToWait(nextCheckToActivateStatus);
                        } else if (!mapActivatePartitions.isEmpty()) {
                            AvroCompactedLogBackupCoordination data = searchData(tp.topic(), tp.partition(), EnumType.ACTIVATE);
                            if (data != null) {
                                mapActivatePartitions.put(tp, data);
                                storeTumbstoneDataCoordinateTopic(tp.partition());
                            }
                        }
                    } else if (!mapActivatePartitions.isEmpty() && mapActivatePartitions.keySet().size() == this.assignment.size()) {
                        for (TopicPartition tp1 : this.assignment) {
                            //RESET OFFSET FOR THIS PARTITION
                            AmazonS3Utils.cleanLastBackup(amazonS3, this.connectorConfig.getBucketName(),
                                    this.connectorConfig.getName(), tp1);
                            this.context.offset(tp1, 0);
                            this.context.requestCommit();
                        }
                        this.initializeData();
                        this.status = StatusConnector.ON_STARTING;
                        this.nextCheckToActivateStatus = null;
                    }
                    break;
                case ON_STARTING:
                    logger.info("ON_STARTING {} - Topic {} Partition {} Offset {}", this.connectorConfig.getName(), topic, partition, offset);
                    if (mapActivatePartitions.get(tp) != null
                            && mapActivatePartitions.get(tp).getOffset() <= offset) {
                        mapActivatePartitions.remove(tp);
                    }
                    topicPartitionWriters.get(tp).buffer(record);
                    for (TopicPartition tp1 : assignment) {
                        topicPartitionWriters.get(tp1).write();
                    }
                    if (mapActivatePartitions.isEmpty()) {
                        this.status = StatusConnector.RUNNING;
                        this.setNextDate();
                        for (TopicPartition tp1 : this.assignment) {
                            storeDataCoordinateTopic(tp1, 0, EnumType.PASSIVATE);
                        }
                    }
                    break;
                case RUNNING:
                    logger.info("RUNNING {} - Topic {} Partition {} Offset {}", this.connectorConfig.getName(), topic, partition, offset);
                    if (elapsedInterval()) {
                        if (this.mapCoordinationTopic.isEmpty() || !this.mapCoordinationTopic.containsKey(tp)) {
                            storeDataCoordinateTopic(tp, offset, EnumType.ACTIVATE);
                            this.mapCoordinationTopic.put(tp, offset);
                        } else if (!this.mapCoordinationTopic.isEmpty() && this.mapCoordinationTopic.keySet().size() == this.assignment.size()) {
                            if (nextCheckToPassivateStatus == null || Calendar.getInstance().after(nextCheckToPassivateStatus)) {
                                AvroCompactedLogBackupCoordination data = searchData(tp.topic(), tp.partition(), EnumType.PASSIVATE);
                                if (data != null) {
                                    mapPassivatePartitions.add(data);
                                    storeTumbstoneDataCoordinateTopic(tp.partition());
                                }
                                nextCheckToPassivateStatus = addTimeToWait(nextCheckToPassivateStatus);
                            }
                        }
                        topicPartitionWriters.get(tp).buffer(record);
                        for (TopicPartition tp1 : assignment) {
                            topicPartitionWriters.get(tp1).write();
                        }
                        if (mapPassivatePartitions.size() == this.assignment.size()) {
                            clearAssignmentAndTopicPartitionWriter();
                            this.status = StatusConnector.WAITING;
                            this.mapCoordinationTopic.clear();
                            this.mapPassivatePartitions.clear();
                            this.nextCheckToPassivateStatus = null;
                        }
                    } else {
                        topicPartitionWriters.get(tp).buffer(record);
                        for (TopicPartition tp1 : assignment) {
                            topicPartitionWriters.get(tp1).write();
                        }
                    }
            }
        }
        if (this.status.equals(StatusConnector.WAITING)) {
            try {
                Thread.sleep(30000);
            } catch (InterruptedException e) {
                logger.error(e.getMessage(), e);
            }
        }
    }

    private String getOtherConnectorInstanceName(int partition) {
        String nameCurrentInstance = this.connectorConfig.getName();
        int indexEnd = nameCurrentInstance.lastIndexOf("-");
        if (indexEnd == -1) {
            this.stop();
            throw new IllegalArgumentException(MessageFormat.format("Naming convention for connector {0} not respected", nameCurrentInstance));
        }
        StringBuilder nameOtherConnectorInstance = new StringBuilder(nameCurrentInstance.substring(0, indexEnd));
        if (this.connectorConfig.getCompactedLogBackupInitialStatusConfig().equals(EnumType.ACTIVATE)) {
            nameOtherConnectorInstance.append("-").append(EnumType.PASSIVATE.toString().toLowerCase());
        } else {
            nameOtherConnectorInstance.append("-").append(EnumType.ACTIVATE.toString().toLowerCase());
        }
        nameOtherConnectorInstance.append("-").append(partition);
        return nameOtherConnectorInstance.toString();
    }

    private void storeDataCoordinateTopic(TopicPartition topicPartition, long offset, EnumType enumType) {
        String key = getOtherConnectorInstanceName(topicPartition.partition());
        AvroCompactedLogBackupCoordination avroCompactedLogBackupCoordination = new AvroCompactedLogBackupCoordination().newBuilder()
                .setConnectorInstanceName(key)
                .setEvent(enumType)
                .setTimestamp(Calendar.getInstance().getTimeInMillis())
                .setTopic(topicPartition.topic())
                .setPartition(topicPartition.partition())
                .setOffset(offset)
                .build();

        try {
            this.kafkaProducer.send(new ProducerRecord<>(TOPIC_COORDINATOR_NAME, key, avroCompactedLogBackupCoordination.toByteBuffer()));
            this.kafkaProducer.flush();
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
    }

    private void storeTumbstoneDataCoordinateTopic(int partition) {
        String key = this.connectorConfig.getName() + "-" + partition;

        this.kafkaProducer.send(new ProducerRecord<>(TOPIC_COORDINATOR_NAME, key, null));
        this.kafkaProducer.flush();

    }

    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
        // No-op. The connector is managing the offsets.
    }

    @Override
    public void close(Collection<TopicPartition> partitions) {
        if (this.status.equals(StatusConnector.RUNNING) || this.status.equals(StatusConnector.ON_STARTING)) {
            this.clearAssignmentAndTopicPartitionWriter();
        }
        this.assignment.clear();
    }

    private void clearAssignmentAndTopicPartitionWriter() {
        for (TopicPartition tp : assignment) {
            topicPartitionWriters.get(tp).close();
        }
        topicPartitionWriters.clear();
    }

    @Override
    public void stop() {
        if (this.kafkaProducer != null) {
            this.kafkaProducer.close();
        }
        if (this.status.equals(StatusConnector.RUNNING) || this.status.equals(StatusConnector.ON_STARTING)) {
            clearAssignmentAndTopicPartitionWriter();
        }
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> preCommit(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
        switch (this.status) {
            case WAITING:
            case ON_STARTING:
                //do nothing
                break;
            case RUNNING:
                for (TopicPartition tp : assignment) {
                    Long offset = topicPartitionWriters.get(tp).getOffsetToCommitAndReset();
                    if (offset != null) {
                        logger.trace("Forwarding to framework request to commit offset: {} for {}", offset, tp);
                        offsetsToCommit.put(tp, new OffsetAndMetadata(offset));
                    }
                }
        }
        return offsetsToCommit;
    }

    private Consumer<String, ByteBuffer> createConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:29092");
        props.put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, "PLAINTEXT");
        props.put(SaslConfigs.SASL_MECHANISM, "GSSAPI");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, this.connectorConfig.getName());
        props.put("schema.registry.url", "http://schema-registry:8081");
        props.put("auto.offset.reset", "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);


        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteBufferDeserializer.class);
        return new KafkaConsumer<>(props);
    }

    private Producer<String, ByteBuffer> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:29092");
        props.put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, "PLAINTEXT");
        props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, this.connectorConfig.getName());
        props.put("schema.registry.url", "http://schema-registry:8081");


        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteBufferSerializer.class);
        return new KafkaProducer<>(props);
    }

    private boolean elapsedInterval() {
        if (Calendar.getInstance().after(nextStart)) {
            return true;
        }
        return false;
    }

    //TODO: Manage time by configuration
    private void setNextDate() {
        this.nextStart = Calendar.getInstance();
        this.nextStart.add(Calendar.HOUR, this.connectorConfig.getS3RetentionInHours());
    }

    private synchronized AvroCompactedLogBackupCoordination searchData(String topic, int partition, EnumType enumType) {
        if (this.kafkaConsumer == null) {
            this.kafkaConsumer = createConsumer();
        }
        AvroCompactedLogBackupCoordination data = null;
        try {
            this.kafkaConsumer.subscribe(Collections.singletonList(TOPIC_COORDINATOR_NAME), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {

                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    kafkaConsumer.seekToBeginning(partitions);
                }
            });
            final int MAX_ATTEMP = 3;
            int noMessageFound = 0;
            while (true) {
                ConsumerRecords<String, ByteBuffer> consumerRecords = this.kafkaConsumer.poll(Duration.ofMillis(1000));
                if (consumerRecords.count() == 0) {
                    noMessageFound++;
                    if (noMessageFound > MAX_ATTEMP)
                        // If no message found count is reached to threshold exit loop.
                        break;
                    else
                        continue;
                }
                for (ConsumerRecord<String, ByteBuffer> record : consumerRecords) {
                    String keyExpected = this.connectorConfig.getName() + "-" + partition;
                    String key = record.key();
                    ByteBuffer value = record.value();
                    if (keyExpected.equalsIgnoreCase(key)) {
                        if (value == null) {
                            data = null;
                            continue;
                        }
                        AvroCompactedLogBackupCoordination avroCompactedLogBackupCoordination = null;
                        try {
                            avroCompactedLogBackupCoordination = AvroCompactedLogBackupCoordination.fromByteBuffer(value);
                        } catch (IOException e) {
                            logger.error(e.getMessage(), e);
                        }
                        if (avroCompactedLogBackupCoordination != null) {
                            if (topic.equalsIgnoreCase(avroCompactedLogBackupCoordination.getTopic().toString())) {
                                if (partition == avroCompactedLogBackupCoordination.getPartition()) {
                                    if (enumType.equals(avroCompactedLogBackupCoordination.getEvent())) {
                                        data = avroCompactedLogBackupCoordination;
                                    }
                                }
                            }
                        }
                    }
                }
            }
            this.kafkaConsumer.unsubscribe();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            if (this.kafkaConsumer != null) {
                this.kafkaConsumer.close();
                this.kafkaConsumer = null;
            }
        }
        return data;
    }
}
