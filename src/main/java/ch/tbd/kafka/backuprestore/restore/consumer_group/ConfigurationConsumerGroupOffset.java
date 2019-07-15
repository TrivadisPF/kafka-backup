package ch.tbd.kafka.backuprestore.restore.consumer_group;

import java.util.HashMap;
import java.util.Map;

/**
 * Class ConfigurationConsumerGroupOffset.
 * This represents TODO.
 *
 * @author iorfinoa
 * @version $$Revision$$
 */
public class ConfigurationConsumerGroupOffset {

    private String oldTopicName;
    private String newTopicName;
    private String consumerGroup;
    private Map<Integer, Long> mapPartitionOldOffset = new HashMap<>();
    private Map<Integer, Long> mapPartitionNewLatestOffset = new HashMap<>();

    public String getOldTopicName() {
        return oldTopicName;
    }

    public void setOldTopicName(String oldTopicName) {
        this.oldTopicName = oldTopicName;
    }

    public String getNewTopicName() {
        return newTopicName;
    }

    public void setNewTopicName(String newTopicName) {
        this.newTopicName = newTopicName;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public Map<Integer, Long> getMapPartitionOldOffset() {
        return mapPartitionOldOffset;
    }

    public void setMapPartitionOldOffset(Map<Integer, Long> mapPartitionOldOffset) {
        this.mapPartitionOldOffset = mapPartitionOldOffset;
    }

    public Map<Integer, Long> getMapPartitionNewLatestOffset() {
        return mapPartitionNewLatestOffset;
    }

    public void setMapPartitionNewLatestOffset(Map<Integer, Long> mapPartitionNewLatestOffset) {
        this.mapPartitionNewLatestOffset = mapPartitionNewLatestOffset;
    }

    public boolean isEqualsTopicName() {
        return oldTopicName.equalsIgnoreCase(newTopicName);
    }

    public static ConfigurationConsumerGroupOffset createConfigurationConsumerGroupOffset(
            String oldTopicName, String newTopicName) {
        ConfigurationConsumerGroupOffset configurationConsumerGroupOffset = new ConfigurationConsumerGroupOffset();
        configurationConsumerGroupOffset.setOldTopicName(oldTopicName);
        configurationConsumerGroupOffset.setNewTopicName(newTopicName);
        return configurationConsumerGroupOffset;
    }

    public static ConfigurationConsumerGroupOffset createConfigurationConsumerGroupOffset(
            String oldTopicName, String newTopicName, String consumerGroup) {
        ConfigurationConsumerGroupOffset configurationConsumerGroupOffset = new ConfigurationConsumerGroupOffset();
        configurationConsumerGroupOffset.setOldTopicName(oldTopicName);
        configurationConsumerGroupOffset.setNewTopicName(newTopicName);
        configurationConsumerGroupOffset.setConsumerGroup(consumerGroup);
        return configurationConsumerGroupOffset;
    }
}
