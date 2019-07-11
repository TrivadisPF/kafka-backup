package ch.tbd.kafka.backuprestore.model;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Class PartitionRecord.
 * This represents TODO.
 *
 * @author iorfinoa
 * @version $$Revision$$
 */
public class PartitionRecord {

    private String s3TopicName;
    private String kafkaTopicName;
    private int numTask;
    private Set<Integer> partitions = new HashSet<>();

    public PartitionRecord() {
    }

    public PartitionRecord(String s3TopicName, String kafkaTopicName) {
        this.s3TopicName = s3TopicName;
        this.kafkaTopicName = kafkaTopicName;
    }

    public String getS3TopicName() {
        return s3TopicName;
    }

    public void setS3TopicName(String s3TopicName) {
        this.s3TopicName = s3TopicName;
    }

    public String getKafkaTopicName() {
        return kafkaTopicName;
    }

    public void setKafkaTopicName(String kafkaTopicName) {
        this.kafkaTopicName = kafkaTopicName;
    }

    public int getNumTask() {
        return numTask;
    }

    public void setNumTask(int numTask) {
        this.numTask = numTask;
    }

    public Set<Integer> getPartitions() {
        return partitions;
    }

    public void setPartitions(Set<Integer> partitions) {
        this.partitions = partitions;
    }

    public int getSizePartitions() {
        return partitions.size();
    }

    public void addPartition(int partition) {
        partitions.add(partition);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PartitionRecord that = (PartitionRecord) o;
        return Objects.equals(s3TopicName, that.s3TopicName) &&
                Objects.equals(kafkaTopicName, that.kafkaTopicName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(s3TopicName, kafkaTopicName);
    }
}
