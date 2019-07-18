package ch.tbd.kafka.backuprestore.restore.consumer_group;

import org.apache.commons.cli.CommandLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Class ConfigConsumerGroupOffsets.
 * This represents TODO.
 *
 * @author iorfinoa
 * @version $$Revision$$
 */
public class ConfigConsumerGroupOffsets {

    private Logger logger = LoggerFactory.getLogger(ConfigConsumerGroupOffsets.class);

    private String topic;
    private String newTopicName;
    private String oldTopicName;
    private String consumerGroupName;
    private String partitionOffsets;
    private Map<Integer, Long> mapOldPartitionOffsets = new HashMap<>();
    private Map<Integer, Long> mapNewPartitionOffsets = new HashMap<>();
    private Properties propertiesKafkaConnection = new Properties();
    private Properties propertiesS3Amazon = new Properties();

    public ConfigConsumerGroupOffsets(CommandLine cmd) {

        loadPropertiesData(cmd.getOptionValue("f"), propertiesKafkaConnection);

        topic = cmd.getOptionValue("t");
        if (topic.indexOf(":") > -1) {
            oldTopicName = topic.split(":")[0];
            newTopicName = topic.split(":")[1];
        } else {
            oldTopicName = topic;
            newTopicName = topic;
        }

        consumerGroupName = cmd.getOptionValue("c");
        if (cmd.hasOption("o")) {
            partitionOffsets = cmd.getOptionValue("o");
            if (partitionOffsets.indexOf(",") > -1) {
                String[] object = partitionOffsets.split(",");
                for (String partitionOffsetString : object) {
                    setOffset(partitionOffsetString);
                }
            } else {
                setOffset(partitionOffsets);
            }
        }

        if (cmd.hasOption("s")) {
            loadPropertiesData(cmd.getOptionValue("s"), propertiesS3Amazon);
        }
    }

    private void loadPropertiesData(String filePath, Properties properties) {
        FileInputStream file = null;
        try {
            file = new FileInputStream(filePath);
            properties.load(file);
        } catch (FileNotFoundException e) {
            logger.error(e.getMessage(), e);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        } finally {
            if (file != null) {
                try {
                    file.close();
                } catch (IOException e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }
    }

    private void setOffset(String partitionOffsetString) {
        if (partitionOffsetString.indexOf(":") == -1) {
            throw new IllegalArgumentException("Partition-Offset data defined wrong");
        }
        String[] array = partitionOffsetString.split(":");
        Integer partition = Integer.parseInt(array[0]);
        Long offset = Long.valueOf(array[1]);
        mapOldPartitionOffsets.put(partition, offset);
    }

    public String getNewTopicName() {
        return newTopicName;
    }

    public String getOldTopicName() {
        return oldTopicName;
    }

    public String getConsumerGroupName() {
        return consumerGroupName;
    }

    public Map<Integer, Long> getMapOldPartitionOffsets() {
        return mapOldPartitionOffsets;
    }

    public Map<Integer, Long> getMapNewPartitionOffsets() {
        return mapNewPartitionOffsets;
    }

    public Properties getPropertiesKafkaConnection() {
        return propertiesKafkaConnection;
    }

    public Properties getPropertiesS3Amazon() {
        return propertiesS3Amazon;
    }

    public boolean isPartitionOffsetDefined() {
        return partitionOffsets != null && !partitionOffsets.isEmpty();
    }

    public boolean isS3AmazonConfigurationDefined() {
        return !propertiesS3Amazon.isEmpty();
    }
}
