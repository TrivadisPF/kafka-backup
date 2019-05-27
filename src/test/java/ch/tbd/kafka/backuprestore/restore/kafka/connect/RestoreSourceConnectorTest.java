package ch.tbd.kafka.backuprestore.restore.kafka.connect;

import ch.tbd.kafka.backuprestore.restore.kafkaconnect.RestoreSourceConnector;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Class RestoreSourceConnectorTest.
 * This represents TODO.
 *
 * @author iorfinoa
 * @version $$Revision$$
 */
public class RestoreSourceConnectorTest {

    RestoreSourceConnector restoreSourceConnector;

    @Mock
    private AmazonS3 amazonS3;

    @BeforeEach
    public void init() {
        restoreSourceConnector = new RestoreSourceConnector();
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testPartitionAssigned() throws Exception {
        S3ObjectSummary sum1 = new S3ObjectSummary();
        sum1.setKey("/test-topic/0");
        S3ObjectSummary sum2 = new S3ObjectSummary();
        sum1.setKey("/test-topic/1");
        S3ObjectSummary sum3 = new S3ObjectSummary();
        sum1.setKey("/test-topic/2");
        S3ObjectSummary sum4 = new S3ObjectSummary();
        sum1.setKey("/test-topic/3");
        S3ObjectSummary sum5 = new S3ObjectSummary();
        sum1.setKey("/test-topic/4");
        S3ObjectSummary sum6 = new S3ObjectSummary();
        sum1.setKey("/test-topic/5");

        ListObjectsV2Result result = new ListObjectsV2Result();
        result.getObjectSummaries().add(sum1);
        result.getObjectSummaries().add(sum2);
        result.getObjectSummaries().add(sum3);
        result.getObjectSummaries().add(sum4);
        result.getObjectSummaries().add(sum5);
        result.getObjectSummaries().add(sum6);

        Mockito.when(amazonS3.listObjectsV2(Mockito.any(ListObjectsV2Request.class))).thenReturn(result);

        Map<String, String> configurationMap = new HashMap<>();
        configurationMap.put("s3.bucket.name", "antonio-aws-test");
        configurationMap.put("s3.region", "eu-central-1");
        configurationMap.put("restore.topic", "test-topic");

        restoreSourceConnector.start(configurationMap);
        restoreSourceConnector.setAmazonS3(amazonS3);


        //TODO: TO complete check
        List<Map<String, String>> listConfig = restoreSourceConnector.taskConfigs(3);
        listConfig.stream().forEach(map -> {

        });

    }
}
