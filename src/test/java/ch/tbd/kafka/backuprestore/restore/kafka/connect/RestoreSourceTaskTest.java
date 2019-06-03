package ch.tbd.kafka.backuprestore.restore.kafka.connect;

import ch.tbd.kafka.backuprestore.restore.kafkaconnect.RestoreSourceTask;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Class RestoreSourceTaskTest.
 * This represents TODO.
 *
 * @author iorfinoa
 * @version $$Revision$$
 */
public class RestoreSourceTaskTest {

    private Logger logger = LoggerFactory.getLogger(RestoreSourceTaskTest.class);
    private RestoreSourceTask restoreSourceTask;


    @BeforeEach
    public void init() {
        Map<String, String> configurationMap = new HashMap<>();
        configurationMap.put("s3.bucket.name", "antonio-aws-test");
        configurationMap.put("s3.region", "eu-central-1");
        configurationMap.put("restore.topic", "test-topic");
        restoreSourceTask = new RestoreSourceTask();
        restoreSourceTask.start(configurationMap);
    }

    @AfterEach
    public void close() {
        restoreSourceTask.stop();
    }

    @Test
    public void restoreData() {
        try {
            restoreSourceTask.poll();
        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
        }
    }
}
