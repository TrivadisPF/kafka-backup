package ch.tbd.kafka.backuprestore.restore.kafkaconnect.config;

import ch.tbd.kafka.backuprestore.common.kafkaconnect.AbstractBaseConnectorConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    public final String DEFAULT_NAME_CONNECTOR = "restore-sink";

    public static final String TOPIC_S3_NAME = "topic.name";
    private static final String TOPIC_S3_DOC = "Topic name which you want to restore. The format is topic-name-on-s3:topic-to-use-on-kafka. The topic name to use on kafka is optional." +
            "Example -> topic-a:topic-b The connector will restore the topic-a from S3 and store the data inside topic-b on kafka" +
            "Example -> topic-a The connector will restore the topic-a from S3 and store the data inside topic-a on kafka";
    private static final String TOPIC_S3_DISPLAY = "Topic name to restore";


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

    public RestoreSourceConnectorConfig(Map<String, String> props) {
        this(conf(), props);
        logger.info("RestoreSourceConnectorConfig(Map<String, String> props)");
    }

    protected RestoreSourceConnectorConfig(ConfigDef conf, Map<String, String> props) {
        super(conf, props);
        logger.info("RestoreSourceConnectorConfig(ConfigDef conf, Map<String, String> props)");
    }

    @Override
    public Object get(String key) {
        return super.get(key);
    }


    public static ConfigDef conf() {
        logger.info("conf()");
        ConfigDef configDef = AbstractBaseConnectorConfig.conf();

        final String group = "restore-s3";
        int orderInGroup = 0;

        configDef.define(
                TOPIC_S3_NAME,
                ConfigDef.Type.STRING,
                Importance.HIGH,
                TOPIC_S3_DOC,
                group,
                ++orderInGroup,
                Width.LONG,
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

        return configDef;
    }


    public String getTopicS3Name() {
        return getTopicNameByIndex(0);
    }

    public String getTopicKafkaName() {
        return getTopicNameByIndex(1);
    }

    public int getPollIntervalMsConfig() {
        return getInt(POLL_INTERVAL_MS_CONFIG);
    }

    public int getBatchMaxRecordsConfig() {
        return getInt(BATCH_MAX_RECORDS_CONFIG);
    }

    private String getTopicNameByIndex(int index) {
        String topic = getString(TOPIC_S3_NAME);
        if (topic.indexOf(":") > -1) {
            return topic.split(":")[index];
        }
        return topic;
    }

}
