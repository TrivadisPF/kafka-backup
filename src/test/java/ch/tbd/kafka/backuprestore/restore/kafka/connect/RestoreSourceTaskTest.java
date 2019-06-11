package ch.tbd.kafka.backuprestore.restore.kafka.connect;

import ch.tbd.kafka.backuprestore.restore.kafkaconnect.RestoreSourceTask;
import ch.tbd.kafka.backuprestore.util.Constants;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Class RestoreSourceTaskTest.
 * This represents TODO.
 *
 * @author iorfinoa
 * @version $$Revision$$
 */
public class RestoreSourceTaskTest {

    private final String S3_BUCKET_NAME = "test-bucket-name";
    private final String S3_REGION = "test-topic";
    private final String S3_TOPIC_NAME = "test-topic";
    private Logger logger = LoggerFactory.getLogger(RestoreSourceTaskTest.class);

    private String exampleResponse = null;

    @InjectMocks
    private RestoreSourceTask restoreSourceTask;

    @Mock
    private SourceTaskContext context;
    @Mock
    private AmazonS3 amazonS3;


    @BeforeEach
    public void init() {
        this.exampleResponse = "wwFSItE3EumKPxR0ZXN0LXRvcGljAACMq7mO5loAEjE3OTQ3OTA5MRQxNTY5ODAxODc2Ag==" + System.lineSeparator() +
                "wwFSItE3EumKPxR0ZXN0LXRvcGljAAKOu7mO5loAFC0yODgwNzMxMjkSNDU4MjEyMjYyAg==" + System.lineSeparator() +
                "wwFSItE3EumKPxR0ZXN0LXRvcGljAAS8vLmO5loAFi0xNDI3Mjc4NTE5EjI5NzQ1MzQxOAI=" + System.lineSeparator();
        MockitoAnnotations.initMocks(this);
        OffsetStorageReader reader = new OffsetStorageReader() {
            @Override
            public <T> Map<String, Object> offset(Map<String, T> partition) {
                return null;
            }

            @Override
            public <T> Map<Map<String, T>, Map<String, Object>> offsets(Collection<Map<String, T>> partitions) {
                return null;
            }
        };
        Mockito.when(context.offsetStorageReader()).thenReturn(reader);
//        Mockito.when(context.offsetStorageReader().offset(Mockito.any())).thenReturn(null);

        String keyObject = "KEY_OBJECT";
        S3ObjectSummary s3ObjectSummary = new S3ObjectSummary();
        s3ObjectSummary.setBucketName(S3_BUCKET_NAME);
        s3ObjectSummary.setKey(keyObject);

        ListObjectsV2Result v2Result = new ListObjectsV2Result();
        v2Result.getObjectSummaries().add(s3ObjectSummary);
        GetObjectRequest getObjectRequest = new GetObjectRequest(S3_BUCKET_NAME, keyObject);
        S3ObjectInputStream s3ObjectInputStream = new S3ObjectInputStream(new ByteArrayInputStream(exampleResponse.getBytes(StandardCharsets.UTF_8)), null);

        Mockito.when(amazonS3.listObjectsV2(Mockito.any(ListObjectsV2Request.class))).thenReturn(v2Result);
        S3Object s3Object = new S3Object();
        s3Object.setObjectContent(s3ObjectInputStream);
        Mockito.when(amazonS3.getObject(getObjectRequest)).thenReturn(s3Object);
        initProperties(null);
    }

    private void initProperties(String topicNameKafka) {
        Map<String, String> configurationMap = new HashMap<>();
        configurationMap.put("s3.bucket.name", S3_BUCKET_NAME);
        configurationMap.put("s3.region", S3_REGION);
        if (topicNameKafka == null) {
            configurationMap.put("topic.name", S3_TOPIC_NAME);
        } else {
            configurationMap.put("topic.name", S3_TOPIC_NAME + ":" + topicNameKafka);
        }
        configurationMap.put(Constants.PARTITION_ASSIGNED_KEY, "0");
        restoreSourceTask.start(configurationMap);
    }

    @AfterEach
    public void close() {
        restoreSourceTask.stop();
    }

    @Test
    public void testRestoredMetadata() {
        try {
            List<SourceRecord> sourceRecordList = restoreSourceTask.poll();
            Assertions.assertNotNull(sourceRecordList);
            Assertions.assertNotEquals(0, sourceRecordList.size());
            Assertions.assertEquals(3, sourceRecordList.size());
            Assertions.assertNotNull(sourceRecordList.get(0).headers());
            Assertions.assertEquals(2, sourceRecordList.get(0).headers().size());
            Assertions.assertNotNull(sourceRecordList.get(0).headers().lastWithName(Constants.KEY_HEADER_RECOVER));
            Assertions.assertTrue((Boolean) sourceRecordList.get(0).headers().lastWithName(Constants.KEY_HEADER_RECOVER).value());
        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
        }
    }

    @Test
    public void testNewTopicName() {
        String newTopic = "new-topic";
        initProperties(newTopic);
        try {
            List<SourceRecord> sourceRecordList = restoreSourceTask.poll();
            Assertions.assertNotNull(sourceRecordList);
            Assertions.assertNotEquals(0, sourceRecordList.size());
            Assertions.assertEquals(3, sourceRecordList.size());
            Assertions.assertNotNull(newTopic, sourceRecordList.get(0).topic());
            Assertions.assertNotNull(newTopic, sourceRecordList.get(1).topic());
            Assertions.assertNotNull(newTopic, sourceRecordList.get(2).topic());
        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
        }
    }

    @Test
    public void restoreData() {
        try {
            List<SourceRecord> sourceRecordList = restoreSourceTask.poll();
            Assertions.assertNotNull(sourceRecordList);
            Assertions.assertNotEquals(0, sourceRecordList.size());
            Assertions.assertEquals(3, sourceRecordList.size());
        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
        }
    }
}
