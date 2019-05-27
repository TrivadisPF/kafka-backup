package ch.tbd.kafka.backuprestore.backup.kafkaconnect;

import ch.tbd.kafka.backuprestore.backup.storage.partitioner.DefaultPartitioner;
import ch.tbd.kafka.backuprestore.backup.storage.partitioner.Partitioner;
import ch.tbd.kafka.backuprestore.model.avro.AvroKafkaRecord;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.Test;
import org.junit.platform.commons.logging.Logger;
import org.junit.platform.commons.logging.LoggerFactory;
import org.powermock.api.mockito.PowerMockito;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.*;

/**
 * Class BackupSinkTaskTest.
 * This represents TODO.
 *
 * @author iorfinoa
 * @version $$Revision$$
 */

public class BackupSinkTaskTest {

    private static final Logger log = LoggerFactory.getLogger(BackupSinkTaskTest.class);

    protected static final String TOPIC = "test-topic";
    protected static final String TOPIC_NEW = "test-topic-new";
    protected static final int PARTITION = 0;
    protected static final int PARTITION2 = 13;
    protected static final int PARTITION3 = 14;
    protected static final TopicPartition TOPIC_PARTITION = new TopicPartition(TOPIC, PARTITION);
    protected static final TopicPartition TOPIC_PARTITION_NEW = new TopicPartition(TOPIC_NEW, PARTITION);
    protected static final TopicPartition TOPIC_PARTITION2 = new TopicPartition(TOPIC, PARTITION2);

    BackupSinkTask task;
    protected MockSinkTaskContext context;
    protected BackupSinkConnectorConfig connectorConfig;
    protected Partitioner<?> partitioner = new DefaultPartitioner<>();

    protected static final Time SYSTEM_TIME = new SystemTime();

    public void setUp(Map<String, String> props) {
        Set<TopicPartition> assignment = new HashSet<>();
        assignment.add(TOPIC_PARTITION);
        assignment.add(TOPIC_PARTITION_NEW);
        context = new MockSinkTaskContext(assignment);
        connectorConfig = PowerMockito.spy(new BackupSinkConnectorConfig(props));
    }

    @Test
    public void testBackupDataByBlock() {

        Map<String, String> props = new HashMap<>();
        props.put("flush.size", "3");
        props.put("s3.bucket.name", "antonio-aws-test");
        props.put("s3.region", "eu-central-1");

        setUp(props);

        task = new BackupSinkTask();

        task = new BackupSinkTask(connectorConfig, context, SYSTEM_TIME, partitioner);
        task.initialize(context);

        task.start(props);
        String record = "record-value" + new Random().nextInt();
        List<SinkRecord> sinkRecordsList = new ArrayList<>();
        sinkRecordsList.add(new SinkRecord(TOPIC, 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA, record, 0, new Date().getTime(), TimestampType.CREATE_TIME));
        sinkRecordsList.add(new SinkRecord(TOPIC, 0, Schema.STRING_SCHEMA, "key1", Schema.STRING_SCHEMA, record, 1, new Date().getTime(), TimestampType.CREATE_TIME));
        sinkRecordsList.add(new SinkRecord(TOPIC, 0, Schema.STRING_SCHEMA, "key2", Schema.STRING_SCHEMA, record, 2, new Date().getTime(), TimestampType.CREATE_TIME));
        sinkRecordsList.add(new SinkRecord(TOPIC_NEW, 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA, record, 0, new Date().getTime(), TimestampType.CREATE_TIME));
        sinkRecordsList.add(new SinkRecord(TOPIC_NEW, 0, Schema.STRING_SCHEMA, "key1", Schema.STRING_SCHEMA, record, 1, new Date().getTime(), TimestampType.CREATE_TIME));
        sinkRecordsList.add(new SinkRecord(TOPIC_NEW, 0, Schema.STRING_SCHEMA, "key2", Schema.STRING_SCHEMA, record, 2, new Date().getTime(), TimestampType.CREATE_TIME));
        task.put(sinkRecordsList);
        task.close(context.assignment());
        task.stop();
    }

    @Test
    public void testBackupDataByInterval() {

        Map<String, String> props = new HashMap<>();
        props.put("rotate.interval.ms", "1");
        props.put("flush.size", "7000");
        props.put("s3.bucket.name", "antonio-aws-test");
        props.put("s3.region", "eu-central-1");

        setUp(props);

        task = new BackupSinkTask();

        task = new BackupSinkTask(connectorConfig, context, SYSTEM_TIME, partitioner);
        task.initialize(context);

        task.start(props);
        String record = "record-value" + new Random().nextInt();
        List<SinkRecord> sinkRecordsList = new ArrayList<>();
        sinkRecordsList.add(new SinkRecord(TOPIC, 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA, record, 0, new Date().getTime(), TimestampType.CREATE_TIME));
        sinkRecordsList.add(new SinkRecord(TOPIC, 0, Schema.STRING_SCHEMA, "key1", Schema.STRING_SCHEMA, record, 1, new Date().getTime(), TimestampType.CREATE_TIME));
        sinkRecordsList.add(new SinkRecord(TOPIC, 0, Schema.STRING_SCHEMA, "key2", Schema.STRING_SCHEMA, record, 2, new Date().getTime(), TimestampType.CREATE_TIME));
        sinkRecordsList.add(new SinkRecord(TOPIC_NEW, 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA, record, 0, new Date().getTime(), TimestampType.CREATE_TIME));
        sinkRecordsList.add(new SinkRecord(TOPIC_NEW, 0, Schema.STRING_SCHEMA, "key1", Schema.STRING_SCHEMA, record, 1, new Date().getTime(), TimestampType.CREATE_TIME));
        sinkRecordsList.add(new SinkRecord(TOPIC_NEW, 0, Schema.STRING_SCHEMA, "key2", Schema.STRING_SCHEMA, record, 2, new Date().getTime(), TimestampType.CREATE_TIME));
        task.put(sinkRecordsList);
        task.close(context.assignment());
        task.stop();

        //verifyData();
    }

    private void verifyData() throws Exception {
        AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();
        builder.withRegion(connectorConfig.getRegionConfig());
        builder.withCredentials(new ProfileCredentialsProvider());
        if (connectorConfig.getProxyUrlConfig() != null && !connectorConfig.getProxyUrlConfig().isEmpty() && connectorConfig.getProxyPortConfig() > 0) {
            ClientConfiguration config = new ClientConfiguration();
            config.setProtocol(Protocol.HTTPS);
            config.setProxyHost(connectorConfig.getProxyUrlConfig());
            config.setProxyPort(connectorConfig.getProxyPortConfig());
            builder.withClientConfiguration(config);
        }
        AmazonS3 amazonS3 = builder.build();
        String bucket = connectorConfig.getBucketName();

        ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(connectorConfig.getBucketName()).withPrefix(TOPIC);


        ListObjectsV2Result listObjectsV2Result = amazonS3.listObjectsV2(req);
        List<S3ObjectSummary> listPartitions = new ArrayList<>(listObjectsV2Result.getObjectSummaries());
        for (S3ObjectSummary obj : listPartitions) {
            GetObjectRequest getObjectRequest = new GetObjectRequest(connectorConfig.getBucketName(), obj.getKey());
            AvroKafkaRecord[] records = toObject(AvroKafkaRecord[].class, IOUtils.toByteArray(amazonS3.getObject(getObjectRequest).getObjectContent()));
            System.out.println(records);
        }
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


    protected static class MockSinkTaskContext implements SinkTaskContext {

        private final Map<TopicPartition, Long> offsets;
        private long timeoutMs;
        private Set<TopicPartition> assignment;

        public MockSinkTaskContext(Set<TopicPartition> assignment) {
            this.offsets = new HashMap<>();
            this.timeoutMs = -1L;
            this.assignment = assignment;
        }

        @Override
        public Map<String, String> configs() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void offset(Map<TopicPartition, Long> offsets) {
            this.offsets.putAll(offsets);
        }

        @Override
        public void offset(TopicPartition tp, long offset) {
            offsets.put(tp, offset);
        }

        public Map<TopicPartition, Long> offsets() {
            return offsets;
        }

        @Override
        public void timeout(long timeoutMs) {
            this.timeoutMs = timeoutMs;
        }

        public long timeout() {
            return timeoutMs;
        }

        @Override
        public Set<TopicPartition> assignment() {
            return assignment;
        }

        public void setAssignment(Set<TopicPartition> nextAssignment) {
            assignment = nextAssignment;
        }

        @Override
        public void pause(TopicPartition... partitions) {
        }

        @Override
        public void resume(TopicPartition... partitions) {
        }

        @Override
        public void requestCommit() {
        }
    }

}
