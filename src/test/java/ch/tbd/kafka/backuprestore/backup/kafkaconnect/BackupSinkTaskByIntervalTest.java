package ch.tbd.kafka.backuprestore.backup.kafkaconnect;

import ch.tbd.kafka.backuprestore.AbstractTest;
import ch.tbd.kafka.backuprestore.backup.kafkaconnect.config.BackupSinkConnectorConfig;
import ch.tbd.kafka.backuprestore.backup.storage.partitioner.DefaultPartitioner;
import ch.tbd.kafka.backuprestore.backup.storage.partitioner.Partitioner;
import ch.tbd.kafka.backuprestore.restore.kafkaconnect.config.RestoreSourceConnectorConfig;
import ch.tbd.kafka.backuprestore.util.AmazonS3Utils;
import ch.tbd.kafka.backuprestore.util.Constants;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.platform.commons.logging.Logger;
import org.junit.platform.commons.logging.LoggerFactory;
import org.powermock.api.mockito.PowerMockito;

import java.text.MessageFormat;
import java.util.*;

/**
 * Class BackupSinkTaskByIntervalTest.
 * This represents TODO.
 *
 * @author iorfinoa
 * @version $$Revision$$
 */

public class BackupSinkTaskByIntervalTest extends AbstractTest {

    private static final Logger log = LoggerFactory.getLogger(BackupSinkTaskByIntervalTest.class);

    protected static final String TOPICS = "topics";
    protected static final int PARTITION = 0;

    protected BackupSinkTask task;
    protected MockSinkTaskContext context;
    protected BackupSinkConnectorConfig connectorConfig;
    protected Partitioner<?> partitioner = new DefaultPartitioner<>();

    protected static final Time SYSTEM_TIME = new SystemTime();

    @Override
    protected List<String> getListPropertyFiles() {
        return Arrays.asList("/BackupSinkTaskByIntervalTest.properties");
    }

    public void setUp() {
        List<String> topics = Arrays.asList(getPropertiesMap().get(TOPICS).split(","));
        TopicPartition TOPIC_PARTITION = new TopicPartition(topics.get(0), PARTITION);
        TopicPartition TOPIC_PARTITION_NEW = new TopicPartition(topics.get(1), PARTITION);
        Set<TopicPartition> assignment = new HashSet<>();
        assignment.add(TOPIC_PARTITION);
        assignment.add(TOPIC_PARTITION_NEW);
        context = new MockSinkTaskContext(assignment);
        connectorConfig = PowerMockito.spy(new BackupSinkConnectorConfig(getPropertiesMap()));
    }


    @Test
    public void testBackupDataByInterval() {
        setUp();

        task = new BackupSinkTask();

        task = new BackupSinkTask(connectorConfig, context, SYSTEM_TIME, partitioner);
        task.initialize(context);

        task.start(getPropertiesMap());
        Assertions.assertEquals("BackupSinkTaskByIntervalTest", this.connectorConfig.getName());
        Assertions.assertEquals("test-junit-topic-interval-1,test-junit-topic-interval-2", this.connectorConfig.getTopics());
        Assertions.assertNotNull(this.connectorConfig.getTopicsList());
        Assertions.assertEquals(2, this.connectorConfig.getTopicsList().size());
        String record = "record-value" + new Random().nextInt();
        List<String> topics = Arrays.asList(getPropertiesMap().get(TOPICS).split(","));
        List<SinkRecord> sinkRecordsList = new ArrayList<>();
        sinkRecordsList.add(new SinkRecord(topics.get(0), 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA, record, 0, new Date().getTime(), TimestampType.CREATE_TIME));
        sinkRecordsList.add(new SinkRecord(topics.get(0), 0, Schema.STRING_SCHEMA, "key1", Schema.STRING_SCHEMA, record, 1, new Date().getTime(), TimestampType.CREATE_TIME));
        sinkRecordsList.add(new SinkRecord(topics.get(0), 0, Schema.STRING_SCHEMA, "key2", Schema.STRING_SCHEMA, record, 2, new Date().getTime(), TimestampType.CREATE_TIME));
        sinkRecordsList.add(new SinkRecord(topics.get(1), 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA, record, 0, new Date().getTime(), TimestampType.CREATE_TIME));
        sinkRecordsList.add(new SinkRecord(topics.get(1), 0, Schema.STRING_SCHEMA, "key1", Schema.STRING_SCHEMA, record, 1, new Date().getTime(), TimestampType.CREATE_TIME));
        sinkRecordsList.add(new SinkRecord(topics.get(1), 0, Schema.STRING_SCHEMA, "key2", Schema.STRING_SCHEMA, record, 2, new Date().getTime(), TimestampType.CREATE_TIME));
        task.put(sinkRecordsList);
        task.close(context.assignment());
        task.stop();
        verifyData(topics.get(0));
        verifyData(topics.get(1));
    }

    private void verifyData(String topicName) {
        addProperty(RestoreSourceConnectorConfig.TOPIC_S3_NAME, topicName);
        RestoreSourceConnectorConfig restoreSourceConnectorConfig = new RestoreSourceConnectorConfig(getPropertiesMap());
        AmazonS3 amazonS3 = AmazonS3Utils.initConnection(restoreSourceConnectorConfig);
        ListObjectsRequest objectsPartitionReq = new ListObjectsRequest().withBucketName(getBucketName()).
                withPrefix(topicName + Constants.S3_KEY_SEPARATOR + 0 + Constants.S3_KEY_SEPARATOR);
        ObjectListing objectListing = amazonS3.listObjects(objectsPartitionReq);
        List<S3ObjectSummary> s3ObjectSummaries = objectListing.getObjectSummaries();
        if (s3ObjectSummaries == null || s3ObjectSummaries.isEmpty() || s3ObjectSummaries.size() > 3) {
            Assertions.fail(MessageFormat.format("Expected object for topic {0} 0, found {1}", topicName, s3ObjectSummaries == null ? null : s3ObjectSummaries.size()));
        }
    }
}
