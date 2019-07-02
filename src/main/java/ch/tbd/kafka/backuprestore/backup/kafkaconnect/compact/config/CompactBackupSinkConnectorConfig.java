package ch.tbd.kafka.backuprestore.backup.kafkaconnect.compact.config;

import ch.tbd.kafka.backuprestore.backup.kafkaconnect.config.BackupSinkConnectorConfig;
import ch.tbd.kafka.backuprestore.model.avro.EnumType;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.common.config.ConfigException;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Class CompactBackupSinkConnectorConfig.
 * This represents TODO.
 *
 * @author iorfinoa
 * @version $$Revision$$
 */
public class CompactBackupSinkConnectorConfig extends BackupSinkConnectorConfig {

    private final String name;

    private static final String COMPACTED_LOG_BACKUP_INITIAL_STATUS_CONFIG = "compacted.log.backup.initial.status";
    private static final String COMPACTED_LOG_BACKUP_INITIAL_STATUS_DOC = "The status allow to the connector to define which connector is active at startup. There is no default value.";
    private static final String COMPACTED_LOG_BACKUP_INITIAL_STATUS_DISPLAY = "Initial status of connector ACTIVE/PASSIVE";

    private static final String COMPACTED_LOG_BACKUP_LENGTH_HOURS_CONFIG = "compacted.log.backup.length.hours";
    private static final String COMPACTED_LOG_BACKUP_LENGTH_HOURS_DOC = "The time interval in hours to verify if other connectors start to sleep or to backup." +
            "The default value is 6 hours";
    private static final int COMPACTED_LOG_BACKUP_LENGTH_HOURS_DEFAULT = 6;
    private static final String COMPACTED_LOG_BACKUP_LENGTH_HOURS_DISPLAY = "Status check Interval (hours)";

    public CompactBackupSinkConnectorConfig(Map<String, String> props) {
        this(conf(), props);
    }

    protected CompactBackupSinkConnectorConfig(ConfigDef conf, Map<String, String> props) {
        super(conf, props);
        this.name = parseName(originalsStrings());
    }

    public static ConfigDef conf() {
        final String group = "compact-backup-s3";
        int orderInGroup = 0;

        ConfigDef configDef = BackupSinkConnectorConfig.conf();

        configDef.define(
                COMPACTED_LOG_BACKUP_INITIAL_STATUS_CONFIG,
                Type.STRING,
                ConfigDef.NO_DEFAULT_VALUE,
                new EnumTypeCompactLogInitialStatusValidator(),
                Importance.HIGH,
                COMPACTED_LOG_BACKUP_INITIAL_STATUS_DOC,
                group,
                ++orderInGroup,
                Width.SHORT,
                COMPACTED_LOG_BACKUP_INITIAL_STATUS_DISPLAY,
                new EnumTypeCompactLogInitialStatusRecommender()
        );


        configDef.define(
                COMPACTED_LOG_BACKUP_LENGTH_HOURS_CONFIG,
                Type.LONG,
                COMPACTED_LOG_BACKUP_LENGTH_HOURS_DEFAULT,
                Importance.HIGH,
                COMPACTED_LOG_BACKUP_LENGTH_HOURS_DOC,
                group,
                ++orderInGroup,
                Width.LONG,
                COMPACTED_LOG_BACKUP_LENGTH_HOURS_DISPLAY
        );
        return configDef;
    }

    public String getName() {
        return name;
    }

    public EnumType getCompactedLogBackupInitialStatusConfig() {
        return EnumType.valueOf(getString(COMPACTED_LOG_BACKUP_INITIAL_STATUS_CONFIG));
    }

    public int getCompactedLogBackupLengthHours() {
        return getInt(COMPACTED_LOG_BACKUP_LENGTH_HOURS_CONFIG);
    }

    protected static String parseName(Map<String, String> props) {
        String nameProp = props.get("name");
        return nameProp != null ? nameProp : "compact-backup-sink";
    }

    private static class EnumTypeCompactLogInitialStatusRecommender implements ConfigDef.Recommender {

        @Override
        public List<Object> validValues(String name, Map<String, Object> parsedConfig) {
            return Arrays.asList(EnumType.ACTIVATE, EnumType.PASSIVATE);
        }

        @Override
        public boolean visible(String name, Map<String, Object> parsedConfig) {
            return true;
        }
    }

    private static class EnumTypeCompactLogInitialStatusValidator implements ConfigDef.Validator {

        @Override
        public void ensureValid(String name, Object value) {
            if (value == null) {
                throw new ConfigException(name, value, "Initial status must be non-null");
            }
            String strValue = (String) value;
            if (EnumType.valueOf(strValue) == null ||
                    (EnumType.valueOf(strValue) != EnumType.ACTIVATE && EnumType.valueOf(strValue) != EnumType.PASSIVATE)) {
                throw new ConfigException(
                        name,
                        value,
                        "Initial status >{}< not allowed. Please use only ACTIVATE or PASSIVATE"
                );
            }
        }

        public String toString() {
            return "[" + EnumType.ACTIVATE + "," + EnumType.PASSIVATE + "]";
        }
    }
}
