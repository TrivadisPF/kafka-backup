package ch.tbd.kafka.backuprestore.backup.kafkaconnect;

import ch.tbd.kafka.backuprestore.backup.storage.partitioner.DefaultPartitioner;
import ch.tbd.kafka.backuprestore.backup.storage.partitioner.Partitioner;
import ch.tbd.kafka.backuprestore.backup.storage.partitioner.TopicPartitionWriter;
import ch.tbd.kafka.backuprestore.util.Version;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class BackupSinkTask extends SinkTask {

    private static final Logger logger = LoggerFactory.getLogger(BackupSinkTask.class);
    private BackupSinkConnectorConfig connectorConfig;
    private final Set<TopicPartition> assignment;
    private final Map<TopicPartition, TopicPartitionWriter> topicPartitionWriters;
    private final Time time;
    private Partitioner<?> partitioner;

    /**
     * No-arg constructor. Used by Connect framework.
     */
    public BackupSinkTask() {
        // no-arg constructor required by Connect framework.
        assignment = new HashSet<>();
        topicPartitionWriters = new HashMap<>();
        time = new SystemTime();
    }

    // visible for testing.
    BackupSinkTask(BackupSinkConnectorConfig connectorConfig, SinkTaskContext context,
                   Time time, Partitioner<?> partitioner) {
        this.assignment = new HashSet<>();
        this.topicPartitionWriters = new HashMap<>();
        this.connectorConfig = connectorConfig;
        this.context = context;
        this.partitioner = partitioner;
        this.time = time;
        open(context.assignment());
    }

    public void start(Map<String, String> props) {
        connectorConfig = new BackupSinkConnectorConfig(props);
        partitioner = new DefaultPartitioner<>();
    }

    @Override
    public void open(Collection<TopicPartition> partitions) {
        assignment.addAll(partitions);
        for (TopicPartition tp : assignment) {
            TopicPartitionWriter writer = new TopicPartitionWriter(
                    tp, connectorConfig, context, partitioner, time
            );
            topicPartitionWriters.put(tp, writer);
        }
    }

    @Override
    public void put(Collection<SinkRecord> records) throws ConnectException {
        for (SinkRecord record : records) {
            String topic = record.topic();
            int partition = record.kafkaPartition();
            TopicPartition tp = new TopicPartition(topic, partition);
            topicPartitionWriters.get(tp).buffer(record);
        }

        for (TopicPartition tp : assignment) {
            topicPartitionWriters.get(tp).write();
        }
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
        for (TopicPartition tp : assignment) {
            topicPartitionWriters.get(tp).close();
        }
        topicPartitionWriters.clear();
        assignment.clear();
    }

    @Override
    public void stop() {

    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> preCommit(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
        for (TopicPartition tp : assignment) {
            Long offset = topicPartitionWriters.get(tp).getOffsetToCommitAndReset();
            if (offset != null) {
                logger.trace("Forwarding to framework request to commit offset: {} for {}", offset, tp);
                offsetsToCommit.put(tp, new OffsetAndMetadata(offset));
            }
        }
        return offsetsToCommit;
    }
}
