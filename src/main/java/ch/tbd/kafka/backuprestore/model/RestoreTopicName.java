package ch.tbd.kafka.backuprestore.model;

/**
 * Class RestoreTopicName.
 * This represents TODO.
 *
 * @author iorfinoa
 * @version $$Revision$$
 */
public class RestoreTopicName {

    private String s3TopicName;
    private String kafkaTopicName;

    public RestoreTopicName(String s3TopicName, String kafkaTopicName) {
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
}
