package ch.tbd.kafka.backuprestore.restore.kafkaconnect.config;

import ch.tbd.kafka.backuprestore.common.kafkaconnect.AbstractBaseConnectorConfig;
import ch.tbd.kafka.backuprestore.model.RestoreTopicName;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Class RestoreSourceConnectorConfig.
 * This represents TODO.
 *
 * @author iorfinoa
 * @version $$Revision$$
 */
public class RestoreSourceConnectorConfig extends AbstractBaseConnectorConfig {
    private static Logger logger = LoggerFactory.getLogger(RestoreSourceConnectorConfig.class);
    private final String name;

    public static final String TOPIC_S3_NAME = "topics";
    private static final String TOPIC_S3_DOC = "Topics names which you want to restore. The format is topic-name-on-s3:topic-to-use-on-kafka." +
            "Please use the ',' character to separate each topic which you want to restore. The topic name to use on kafka is optional." +
            "Example -> topic-a:topic-b,topic-c:topic-d The connector will restore the topic-a from S3 and store the data inside topic-b on kafka and the topic-c in topic-d" +
            "Example -> topic-a:topic-b The connector will restore the topic-a from S3 and store the data inside topic-b on kafka" +
            "Example -> topic-a The connector will restore the topic-a from S3 and store the data inside topic-a on kafka";
    private static final String TOPIC_S3_DISPLAY = "Topics names to restore";


    public static final String POLL_INTERVAL_MS_CONFIG = "poll.interval.ms";
    private static final String POLL_INTERVAL_MS_DOC = "Frequency in ms to poll for new data from S3.";
    public static final int POLL_INTERVAL_MS_DEFAULT = 5000;
    private static final String POLL_INTERVAL_MS_DISPLAY = "Poll Interval (ms)";

    public static final String BATCH_MAX_RECORDS_CONFIG = "batch.max.record";
    private static final String BATCH_MAX_RECORDS_DOC =
            "Maximum number of records to include in a single batch when polling for new data. This "
                    + "setting can be used to limit the amount of data buffered internally in the connector.";
    public static final int BATCH_MAX_RECORDS_DEFAULT = 100;
    private static final String BATCH_MAX_RECORDS_DISPLAY = "Max Record per execution to commit";

    public static final String INSTANCE_NAME_TO_RESTORE_CONFIG = "instance.name.restore";
    private static final String INSTANCE_NAME_TO_RESTORE_DOC =
            "Represent the name of the instance to restore in case the backup is related to a compacted topic.";
    private static final String INSTANCE_NAME_TO_RESTORE_DEFAULT = null;
    private static final String INSTANCE_NAME_TO_RESTORE_DISPLAY = "Name instance to restore (only for compacted topic backup)";

    public RestoreSourceConnectorConfig(Map<String, String> props) {
        this(conf(), props);
        logger.info("RestoreSourceConnectorConfig(Map<String, String> props)");
    }

    protected RestoreSourceConnectorConfig(ConfigDef conf, Map<String, String> props) {
        super(conf, props);
        this.name = parseName(originalsStrings());
        logger.info("RestoreSourceConnectorConfig(ConfigDef conf, Map<String, String> props)");
    }

    public static ConfigDef conf() {
        logger.info("conf()");
        ConfigDef configDef = AbstractBaseConnectorConfig.conf();

        final String group = "restore-s3";
        int orderInGroup = 0;

        configDef.define(
                TOPIC_S3_NAME,
                Type.LIST,
                Importance.HIGH,
                TOPIC_S3_DOC,
                group,
                ++orderInGroup,
                Width.SHORT,
                TOPIC_S3_DISPLAY);

        configDef.define(
                POLL_INTERVAL_MS_CONFIG,
                Type.INT,
                POLL_INTERVAL_MS_DEFAULT,
                Importance.HIGH,
                POLL_INTERVAL_MS_DOC,
                group,
                ++orderInGroup,
                Width.SHORT,
                POLL_INTERVAL_MS_DISPLAY);

        configDef.define(
                BATCH_MAX_RECORDS_CONFIG,
                Type.INT,
                BATCH_MAX_RECORDS_DEFAULT,
                Importance.HIGH,
                BATCH_MAX_RECORDS_DOC,
                group,
                ++orderInGroup,
                Width.SHORT,
                BATCH_MAX_RECORDS_DISPLAY);

        configDef.define(
                INSTANCE_NAME_TO_RESTORE_CONFIG,
                Type.STRING,
                INSTANCE_NAME_TO_RESTORE_DEFAULT,
                Importance.MEDIUM,
                INSTANCE_NAME_TO_RESTORE_DOC,
                group,
                ++orderInGroup,
                Width.SHORT,
                INSTANCE_NAME_TO_RESTORE_DISPLAY);


        return configDef;
    }

    public String getName() {
        return name;
    }

    public int getPollIntervalMsConfig() {
        return getInt(POLL_INTERVAL_MS_CONFIG);
    }

    public int getBatchMaxRecordsConfig() {
        return getInt(BATCH_MAX_RECORDS_CONFIG);
    }

    public boolean isInstanceNameToRestoreConfigDefined() {
        return getString(INSTANCE_NAME_TO_RESTORE_CONFIG) != null && !getString(INSTANCE_NAME_TO_RESTORE_CONFIG).isEmpty();
    }

    public String getInstanceNameToRestoreConfig() {
        return getString(INSTANCE_NAME_TO_RESTORE_CONFIG);
    }

    public List<RestoreTopicName> getTopicName() {
        List<String> topicsList = getList(TOPIC_S3_NAME);
        List<RestoreTopicName> restoreTopicNameList = new ArrayList<>();
        for (String topic : topicsList) {
            restoreTopicNameList.add(new RestoreTopicName(getTopicNameByIndex(topic, 0), getTopicNameByIndex(topic, 1)));
        }
        return restoreTopicNameList;
    }

    private String getTopicNameByIndex(String topicName, int index) {
        if (topicName != null && topicName.indexOf(":") > -1 && topicName.split(":").length > index) {
            return topicName.split(":")[index];
        }
        return topicName;
    }

    protected static String parseName(Map<String, String> props) {
        String nameProp = props.get("name");
        return nameProp != null ? nameProp : "restore-source";
    }

}
