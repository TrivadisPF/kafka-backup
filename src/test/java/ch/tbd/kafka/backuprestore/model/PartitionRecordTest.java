package ch.tbd.kafka.backuprestore.model;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

/**
 * Class PartitionRecordTest.
 * This represents TODO.
 *
 * @author iorfinoa
 * @version $$Revision$$
 */
public class PartitionRecordTest {

    private PartitionRecord partitionRecord;

    @BeforeEach
    public void init() {
        this.partitionRecord = new PartitionRecord();
    }

    @Test
    public void testGetPartitions() {
        Assertions.assertNotNull(this.partitionRecord.getPartitions());
        Assertions.assertEquals(0, this.partitionRecord.getPartitions().size());
    }

    @Test
    public void testSetPartitions() {
        Assertions.assertNotNull(this.partitionRecord.getPartitions());
        this.partitionRecord.addPartition(1);
        Assertions.assertEquals(1, this.partitionRecord.getPartitions().size());
        Set<Integer> partitions = new HashSet<Integer>();
        partitions.add(10);
        partitions.add(20);
        partitions.add(30);
        this.partitionRecord.setPartitions(partitions);
        Assertions.assertEquals(3, this.partitionRecord.getPartitions().size());
    }

    @Test
    public void testGetNumTask() {
        Assertions.assertEquals(0, this.partitionRecord.getNumTask());
    }

    @Test
    public void testSetNumTask() {
        Assertions.assertEquals(0, this.partitionRecord.getNumTask());
        this.partitionRecord.setNumTask(10);
        Assertions.assertEquals(10, this.partitionRecord.getNumTask());
    }

    @Test
    public void testGetKafkaTopicName() {
        Assertions.assertNull(this.partitionRecord.getKafkaTopicName());
    }

    @Test
    public void testSetKafkaTopicName() {
        Assertions.assertNull(this.partitionRecord.getKafkaTopicName());
        this.partitionRecord.setKafkaTopicName("kafka-topic-name");
        Assertions.assertEquals("kafka-topic-name", this.partitionRecord.getKafkaTopicName());
        this.partitionRecord = new PartitionRecord(null, "kafka-topic-name-1");
        Assertions.assertEquals("kafka-topic-name-1", this.partitionRecord.getKafkaTopicName());
    }

    @Test
    public void testGetS3TopicName() {
        Assertions.assertNull(this.partitionRecord.getS3TopicName());
        this.partitionRecord.setS3TopicName("s3-topic-name");
        Assertions.assertEquals("s3-topic-name", this.partitionRecord.getS3TopicName());
        this.partitionRecord = new PartitionRecord("s3-topic-name-1", null);
        Assertions.assertEquals("s3-topic-name-1", this.partitionRecord.getS3TopicName());
    }

    @Test
    public void testGetSizePartitions() {
        Assertions.assertEquals(0, this.partitionRecord.getSizePartitions());
        this.partitionRecord.addPartition(10);
        Assertions.assertEquals(1, this.partitionRecord.getSizePartitions());
    }

    @Test
    public void testEquals() {
        this.partitionRecord.setS3TopicName("s3-topic-name");
        this.partitionRecord.setKafkaTopicName("kafka-topic-name");

        PartitionRecord newPartitionRecord = new PartitionRecord("s3-topic-name-1", "kafka-topic-name-1");
        Assertions.assertTrue(this.partitionRecord.equals(partitionRecord));
        Assertions.assertFalse(this.partitionRecord.equals(null));
        Assertions.assertFalse(this.partitionRecord.equals(new RestoreTopicName(null, null)));
        Assertions.assertFalse(this.partitionRecord.equals(newPartitionRecord));
        newPartitionRecord = new PartitionRecord("s3-topic-name1", "kafka-topic-name");
        Assertions.assertFalse(this.partitionRecord.equals(newPartitionRecord));
        newPartitionRecord = new PartitionRecord("s3-topic-name", "kafka-topic-name1");
        Assertions.assertFalse(this.partitionRecord.equals(newPartitionRecord));
        newPartitionRecord = new PartitionRecord("s3-topic-name", "kafka-topic-name");
        Assertions.assertTrue(this.partitionRecord.equals(newPartitionRecord));
    }

    @Test
    public void testHash() {
        this.partitionRecord.setS3TopicName("s3-topic-name");
        this.partitionRecord.setKafkaTopicName("kafka-topic-name");
        Assertions.assertEquals(-552820293, this.partitionRecord.hashCode());
    }

}
