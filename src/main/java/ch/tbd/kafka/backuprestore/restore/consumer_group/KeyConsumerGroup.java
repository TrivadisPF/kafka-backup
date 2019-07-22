package ch.tbd.kafka.backuprestore.restore.consumer_group;

/**
 * Class KeyConsumerGroup.
 * This represents TODO.
 *
 * @author iorfinoa
 * @version $$Revision$$
 */
public class KeyConsumerGroup {

    private short version;
    private String group;
    private String topic;
    private int partition = -1;

    public KeyConsumerGroup(short version, String group) {
        this.version = version;
        this.group = group;
    }

    public KeyConsumerGroup(short version, String group, String topic, int partition) {
        this.version = version;
        this.group = group;
        this.topic = topic;
        this.partition = partition;
    }

    public short getVersion() {
        return version;
    }

    public String getGroup() {
        return group;
    }

    public String getTopic() {
        return topic;
    }

    public int getPartition() {
        return partition;
    }

    public boolean validRecord() {
        return partition > -1 && topic != null && !topic.isEmpty();
    }
}
