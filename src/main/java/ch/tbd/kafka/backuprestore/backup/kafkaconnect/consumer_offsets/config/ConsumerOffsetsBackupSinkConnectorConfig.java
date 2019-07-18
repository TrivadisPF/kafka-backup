package ch.tbd.kafka.backuprestore.backup.kafkaconnect.consumer_offsets.config;

import ch.tbd.kafka.backuprestore.backup.kafkaconnect.config.BackupSinkConnectorConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;

import java.util.Map;

/**
 * Class BackupSinkConnectorConfig.
 * This represents TODO.
 *
 * @author iorfinoa
 * @version $$Revision$$
 */
public class ConsumerOffsetsBackupSinkConnectorConfig extends BackupSinkConnectorConfig {


    private static final String CONSUMER_GROUP_NAME_EXCLUDE_CONFIG = "consumer.group.exclude";
    private static final String CONSUMER_GROUP_NAME_EXCLUDE_DOC =
            "The consumer group name to exclude on the backup operation";
    private static final String CONSUMER_GROUP_NAME_EXCLUDE_DISPLAY = "Consumer group name to exclude";

    private final String name;

    public ConsumerOffsetsBackupSinkConnectorConfig(Map<String, String> props) {
        this(conf(), props);
    }

    protected ConsumerOffsetsBackupSinkConnectorConfig(ConfigDef conf, Map<String, String> props) {
        super(conf, props);
        this.name = parseName(originalsStrings());
    }

    public String getName() {
        return name;
    }

    public static ConfigDef conf() {
        final String group = "consumer-group-backup-s3";
        int orderInGroup = 0;

        ConfigDef configDef = BackupSinkConnectorConfig.conf();

        configDef.define(
                CONSUMER_GROUP_NAME_EXCLUDE_CONFIG,
                Type.STRING,
                Importance.HIGH,
                CONSUMER_GROUP_NAME_EXCLUDE_DOC,
                group,
                ++orderInGroup,
                Width.LONG,
                CONSUMER_GROUP_NAME_EXCLUDE_DISPLAY
        );
        return configDef;
    }

    public String getConsumerGroupNameExcludeConfig() {
        return getString(CONSUMER_GROUP_NAME_EXCLUDE_CONFIG);
    }

    protected static String parseName(Map<String, String> props) {
        String nameProp = props.get("name");
        return nameProp != null ? nameProp : "consumer-group-backup-sink";
    }


}

