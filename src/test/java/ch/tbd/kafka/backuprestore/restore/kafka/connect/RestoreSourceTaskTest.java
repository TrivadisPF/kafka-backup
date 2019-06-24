package ch.tbd.kafka.backuprestore.restore.kafka.connect;

import ch.tbd.kafka.backuprestore.AbstractTest;
import ch.tbd.kafka.backuprestore.restore.kafkaconnect.RestoreSourceTask;
import ch.tbd.kafka.backuprestore.restore.kafkaconnect.config.RestoreSourceConnectorConfig;
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
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Class RestoreSourceTaskTest.
 * This represents TODO.
 *
 * @author iorfinoa
 * @version $$Revision$$
 */
public class RestoreSourceTaskTest extends AbstractTest {

    private Logger logger = LoggerFactory.getLogger(RestoreSourceTaskTest.class);

    @InjectMocks
    private RestoreSourceTask restoreSourceTask;

    @Mock
    private SourceTaskContext context;
    @Mock
    private AmazonS3 amazonS3;


    @Override
    protected List<String> getListPropertyFiles() {
        return Arrays.asList("/RestoreSourceTaskTest.properties");
    }

    @BeforeEach
    public void init() throws IOException {
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

        String keyObject = "KEY_OBJECT";
        S3ObjectSummary s3ObjectSummary = new S3ObjectSummary();
        s3ObjectSummary.setBucketName(getBucketName());
        s3ObjectSummary.setKey(keyObject);

        ObjectListing result = new ObjectListing();
        result.getObjectSummaries().add(s3ObjectSummary);
        GetObjectRequest getObjectRequest = new GetObjectRequest(getBucketName(), keyObject);
        byte[] inputFile = {79, 98, 106, 1, 4, 22, 97, 118, 114, 111, 46, 115, 99, 104, 101, 109, 97, -78, 6, 123, 34,
                116, 121, 112, 101, 34, 58, 34, 114, 101, 99, 111, 114, 100, 34, 44, 34, 110, 97, 109, 101, 34, 58, 34,
                65, 118, 114, 111, 75, 97, 102, 107, 97, 82, 101, 99, 111, 114, 100, 34, 44, 34, 110, 97, 109, 101, 115,
                112, 97, 99, 101, 34, 58, 34, 99, 104, 46, 116, 98, 100, 46, 107, 97, 102, 107, 97, 46, 98, 97, 99, 107,
                117, 112, 114, 101, 115, 116, 111, 114, 101, 46, 109, 111, 100, 101, 108, 46, 97, 118, 114, 111, 34, 44,
                34, 102, 105, 101, 108, 100, 115, 34, 58, 91, 123, 34, 110, 97, 109, 101, 34, 58, 34, 116, 111, 112,
                105, 99, 34, 44, 34, 116, 121, 112, 101, 34, 58, 34, 115, 116, 114, 105, 110, 103, 34, 125, 44, 123, 34,
                110, 97, 109, 101, 34, 58, 34, 112, 97, 114, 116, 105, 116, 105, 111, 110, 34, 44, 34, 116, 121, 112,
                101, 34, 58, 34, 105, 110, 116, 34, 125, 44, 123, 34, 110, 97, 109, 101, 34, 58, 34, 111, 102, 102, 115,
                101, 116, 34, 44, 34, 116, 121, 112, 101, 34, 58, 34, 108, 111, 110, 103, 34, 125, 44, 123, 34, 110,
                97, 109, 101, 34, 58, 34, 116, 105, 109, 101, 115, 116, 97, 109, 112, 34, 44, 34, 116, 121, 112, 101,
                34, 58, 34, 108, 111, 110, 103, 34, 125, 44, 123, 34, 110, 97, 109, 101, 34, 58, 34, 107, 101, 121,
                34, 44, 34, 116, 121, 112, 101, 34, 58, 91, 34, 98, 121, 116, 101, 115, 34, 44, 34, 110, 117, 108,
                108, 34, 93, 44, 34, 100, 101, 102, 97, 117, 108, 116, 34, 58, 34, 110, 117, 108, 108, 34, 125, 44,
                123, 34, 110, 97, 109, 101, 34, 58, 34, 118, 97, 108, 117, 101, 34, 44, 34, 116, 121, 112, 101, 34,
                58, 34, 98, 121, 116, 101, 115, 34, 125, 44, 123, 34, 110, 97, 109, 101, 34, 58, 34, 104, 101, 97, 100,
                101, 114, 115, 34, 44, 34, 116, 121, 112, 101, 34, 58, 91, 123, 34, 116, 121, 112, 101, 34, 58, 34,
                109, 97, 112, 34, 44, 34, 118, 97, 108, 117, 101, 115, 34, 58, 34, 98, 121, 116, 101, 115, 34, 125, 44,
                34, 110, 117, 108, 108, 34, 93, 44, 34, 100, 101, 102, 97, 117, 108, 116, 34, 58, 110, 117, 108, 108,
                125, 93, 125, 20, 97, 118, 114, 111, 46, 99, 111, 100, 101, 99, 8, 110, 117, 108, 108, 0, 66, -5, -102,
                50, -28, -51, 62, 11, -56, 103, 127, 12, -54, -77, 113, 62, 6, -52, 4, 20, 116, 101, 115, 116, 45, 116,
                111, 112, 105, 99, 0, 18, -92, -119, -38, -53, -20, 90, 0, 74, -84, -19, 0, 5, 117, 114, 0, 2, 91, 66,
                -84, -13, 23, -8, 6, 8, 84, -32, 2, 0, 0, 120, 112, 0, 0, 0, 10, 45, 49, 52, 52, 51, 55, 53, 48, 54,
                57, 76, -84, -19, 0, 5, 117, 114, 0, 2, 91, 66, -84, -13, 23, -8, 6, 8, 84, -32, 2, 0, 0, 120, 112, 0,
                0, 0, 11, 45, 49, 54, 52, 52, 54, 50, 55, 55, 56, 49, 0, 0, 20, 116, 101, 115, 116, 45, 116, 111, 112,
                105, 99, 0, 20, -46, -119, -38, -53, -20, 90, 0, 74, -84, -19, 0, 5, 117, 114, 0, 2, 91, 66, -84, -13,
                23, -8, 6, 8, 84, -32, 2, 0, 0, 120, 112, 0, 0, 0, 10, 45, 54, 48, 56, 54, 48, 53, 48, 53, 49, 72, -84,
                -19, 0, 5, 117, 114, 0, 2, 91, 66, -84, -13, 23, -8, 6, 8, 84, -32, 2, 0, 0, 120, 112, 0, 0, 0, 9, 52,
                56, 53, 53, 56, 55, 52, 50, 51, 0, 0, 20, 116, 101, 115, 116, 45, 116, 111, 112, 105, 99, 0, 22, -76,
                -118, -38, -53, -20, 90, 0, 74, -84, -19, 0, 5, 117, 114, 0, 2, 91, 66, -84, -13, 23, -8, 6, 8, 84, -32,
                2, 0, 0, 120, 112, 0, 0, 0, 10, 49, 52, 51, 50, 56, 52, 57, 49, 50, 50, 74, -84, -19, 0, 5, 117, 114,
                0, 2, 91, 66, -84, -13, 23, -8, 6, 8, 84, -32, 2, 0, 0, 120, 112, 0, 0, 0, 10, 49, 57, 54, 54, 55, 48,
                57, 51, 54, 50, 0, 0, 66, -5, -102, 50, -28, -51, 62, 11, -56, 103, 127, 12, -54, -77, 113, 62};
        S3ObjectInputStream s3ObjectInputStream = new S3ObjectInputStream(new ByteArrayInputStream(inputFile), null);
        Mockito.when(amazonS3.listObjects(Mockito.any(ListObjectsRequest.class))).thenReturn(result);
        S3Object s3Object = new S3Object();
        s3Object.setObjectContent(s3ObjectInputStream);
        Mockito.when(amazonS3.getObject(getObjectRequest)).thenReturn(s3Object);
        initProperties(null);
    }

    private byte[] toByteArray(InputStream inputStream) throws IOException {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        int len;
        while ((len = inputStream.read(buffer)) != -1) {
            os.write(buffer, 0, len);
        }
        return os.toByteArray();
    }

    private void initProperties(String topicNameKafka) {
        if (topicNameKafka != null) {
            addProperty(RestoreSourceConnectorConfig.TOPIC_S3_NAME, getPropertiesMap().get(RestoreSourceConnectorConfig.TOPIC_S3_NAME) + ":" + topicNameKafka);
        }
        addProperty(Constants.KEY_PARTITION_TASK, "0");
        restoreSourceTask.start(getPropertiesMap());
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
            Assertions.fail(e.getMessage());
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
            Assertions.fail(e.getMessage());
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
            Assertions.fail(e.getMessage());
        }
    }
}
