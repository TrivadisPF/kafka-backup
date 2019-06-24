package ch.tbd.kafka.backuprestore.model;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Class RestoreTopicNameTest.
 * This represents TODO.
 *
 * @author iorfinoa
 * @version $$Revision$$
 */
public class RestoreTopicNameTest {

    private RestoreTopicName restoreTopicName;

    @BeforeEach
    public void init() {
        this.restoreTopicName = new RestoreTopicName(null, null);
    }

    @Test
    public void testGetKafkaTopicName() {
        Assertions.assertNull(this.restoreTopicName.getKafkaTopicName());
    }

    @Test
    public void testSetKafkaTopicName() {
        this.restoreTopicName.setKafkaTopicName("kafka-topic-name");
        Assertions.assertEquals("kafka-topic-name", this.restoreTopicName.getKafkaTopicName());
        this.restoreTopicName = new RestoreTopicName(null, "kafka-topic-name-1");
        Assertions.assertEquals("kafka-topic-name-1", this.restoreTopicName.getKafkaTopicName());
    }

    @Test
    public void testGetS3TopicName() {
        Assertions.assertNull(this.restoreTopicName.getS3TopicName());
    }

    @Test
    public void testSetS3TopicName() {
        this.restoreTopicName.setS3TopicName("s3-topic-name");
        Assertions.assertEquals("s3-topic-name", this.restoreTopicName.getS3TopicName());
        this.restoreTopicName = new RestoreTopicName("s3-topic-name-1", null);
        Assertions.assertEquals("s3-topic-name-1", this.restoreTopicName.getS3TopicName());
    }
}
