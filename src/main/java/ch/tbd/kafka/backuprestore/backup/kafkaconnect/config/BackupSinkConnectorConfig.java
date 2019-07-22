package ch.tbd.kafka.backuprestore.backup.kafkaconnect.config;

import ch.tbd.kafka.backuprestore.backup.storage.CompressionType;
import ch.tbd.kafka.backuprestore.common.kafkaconnect.AbstractBaseConnectorConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.sink.SinkTask;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Class BackupSinkConnectorConfig.
 * This represents TODO.
 *
 * @author iorfinoa
 * @version $$Revision$$
 */
public class BackupSinkConnectorConfig extends AbstractBaseConnectorConfig {

    private static final String PART_SIZE_CONFIG = "s3.part.size";
    private static final int PART_SIZE_DEFAULT = 25 * 1024 * 1024;

    private static final String COMPRESSION_TYPE_CONFIG = "s3.compression.type";
    private static final String COMPRESSION_TYPE_DEFAULT = "none";

    private static final String FLUSH_SIZE_CONFIG = "flush.size";
    private static final String FLUSH_SIZE_DOC =
            "Number of records written to store before invoking file commits.";
    private static final String FLUSH_SIZE_DISPLAY = "Flush Size";

    private static final String RETRY_BACKOFF_CONFIG = "retry.backoff.ms";
    private static final String
            RETRY_BACKOFF_DOC =
            "The retry backoff in milliseconds. This config is used to notify Kafka connect to retry "
                    + "delivering a message batch or performing recovery in case of transient exceptions.";
    private static final long RETRY_BACKOFF_DEFAULT = 5000L;
    private static final String RETRY_BACKOFF_DISPLAY = "Retry Backoff (ms)";

    private static final String FILENAME_OFFSET_ZERO_PAD_WIDTH_CONFIG =
            "filename.offset.zero.pad.width";
    private static final String
            FILENAME_OFFSET_ZERO_PAD_WIDTH_DOC =
            "Width to zero-pad offsets in store's filenames if offsets are too short in order to "
                    + "provide fixed-width filenames that can be ordered by simple lexicographic sorting.";
    private static final int FILENAME_OFFSET_ZERO_PAD_WIDTH_DEFAULT = 10;
    private static final String FILENAME_OFFSET_ZERO_PAD_WIDTH_DISPLAY =
            "Filename Offset Zero Pad Width";

    private static final String S3_RETENTION_POLICY_CONFIG = "s3.retention.policy.days";
    private static final String
            S3_RETENTION_POLICY_DOC =
            "The retention in days. It will be used to set the rules on S3 in order to clean the data inside the bucket. Default is 1 day";
    private static final int S3_RETENTION_POLICY_DEFAULT = 1;
    private static final String S3_RETENTION_POLICY_DISPLAY = "Retention on S3 bucket (days)";

    private final String name;

    public BackupSinkConnectorConfig(Map<String, String> props) {
        this(conf(), props);
    }

    protected BackupSinkConnectorConfig(ConfigDef conf, Map<String, String> props) {
        super(conf, props);
        this.name = parseName(originalsStrings());
    }

    public String getName() {
        return name;
    }

    public String getTopics() {
        return getString(SinkTask.TOPICS_CONFIG);
    }

    public List<String> getTopicsList() {
        List<String> topics = new ArrayList<>();
        String confTopic = getString(SinkTask.TOPICS_CONFIG);
        if (confTopic.contains(",")) {
            String[] array = confTopic.split(",");
            for (String topicTmp : array) {
                topics.add(topicTmp);
            }
        } else {
            topics.add(getTopics());
        }
        return topics;
    }

    public static ConfigDef conf() {
        final String group = "backup-s3";
        int orderInGroup = 0;

        ConfigDef configDef = AbstractBaseConnectorConfig.conf();

        configDef.define(
                SinkTask.TOPICS_CONFIG,
                Type.STRING,
                ConfigDef.NO_DEFAULT_VALUE,
                Importance.HIGH,
                "",
                group,
                ++orderInGroup,
                Width.LONG,
                ""
        );

        configDef.define(
                PART_SIZE_CONFIG,
                Type.INT,
                PART_SIZE_DEFAULT,
                new PartRange(),
                Importance.HIGH,
                "The Part Size in S3 Multi-part Uploads.",
                group,
                ++orderInGroup,
                Width.LONG,
                "S3 Part Size"
        );

        configDef.define(
                COMPRESSION_TYPE_CONFIG,
                Type.STRING,
                COMPRESSION_TYPE_DEFAULT,
                new CompressionTypeValidator(),
                Importance.LOW,
                "Compression type for file written to S3. "
                        + "Applied when using JsonFormat or ByteArrayFormat. "
                        + "Available values: none, gzip.",
                group,
                ++orderInGroup,
                Width.LONG,
                "Compression type"
        );

        configDef.define(
                FLUSH_SIZE_CONFIG,
                Type.INT,
                Importance.HIGH,
                FLUSH_SIZE_DOC,
                group,
                ++orderInGroup,
                Width.LONG,
                FLUSH_SIZE_DISPLAY
        );

        configDef.define(
                RETRY_BACKOFF_CONFIG,
                Type.LONG,
                RETRY_BACKOFF_DEFAULT,
                Importance.LOW,
                RETRY_BACKOFF_DOC,
                group,
                ++orderInGroup,
                Width.MEDIUM,
                RETRY_BACKOFF_DISPLAY
        );

        configDef.define(
                FILENAME_OFFSET_ZERO_PAD_WIDTH_CONFIG,
                Type.INT,
                FILENAME_OFFSET_ZERO_PAD_WIDTH_DEFAULT,
                ConfigDef.Range.atLeast(0),
                Importance.LOW,
                FILENAME_OFFSET_ZERO_PAD_WIDTH_DOC,
                group,
                ++orderInGroup,
                Width.LONG,
                FILENAME_OFFSET_ZERO_PAD_WIDTH_DISPLAY
        );

        configDef.define(
                S3_RETENTION_POLICY_CONFIG,
                Type.INT,
                S3_RETENTION_POLICY_DEFAULT,
                ConfigDef.Range.atLeast(0),
                Importance.LOW,
                S3_RETENTION_POLICY_DOC,
                group,
                ++orderInGroup,
                Width.LONG,
                S3_RETENTION_POLICY_DISPLAY
        );

        return configDef;
    }

    protected static String parseName(Map<String, String> props) {
        String nameProp = props.get("name");
        return nameProp != null ? nameProp : "backup-sink";
    }

    public int getPartSize() {
        return getInt(PART_SIZE_CONFIG);
    }


    public CompressionType getCompressionType() {
        return CompressionType.forName(getString(COMPRESSION_TYPE_CONFIG));
    }

    public int getFlushSize() {
        return getInt(BackupSinkConnectorConfig.FLUSH_SIZE_CONFIG);
    }

    public long getRetryBackoffDefault() {
        return getLong(RETRY_BACKOFF_CONFIG);
    }

    public int getS3RetentionInDays() {
        return getInt(S3_RETENTION_POLICY_CONFIG);
    }

    private static class PartRange implements ConfigDef.Validator {
        // S3 specific limit
        final int min = 5 * 1024 * 1024;
        // Connector specific
        final int max = Integer.MAX_VALUE;

        @Override
        public void ensureValid(String name, Object value) {
            if (value == null) {
                throw new ConfigException(name, value, "Part size must be non-null");
            }
            Number number = (Number) value;
            if (number.longValue() < min) {
                throw new ConfigException(
                        name,
                        value,
                        "Part size must be at least: " + min + " bytes (5MB)"
                );
            }
            if (number.longValue() > max) {
                throw new ConfigException(
                        name,
                        value,
                        "Part size must be no more: " + Integer.MAX_VALUE + " bytes (~2GB)"
                );
            }
        }

        public String toString() {
            return "[" + min + ",...," + max + "]";
        }
    }


    private static class CompressionTypeValidator implements ConfigDef.Validator {
        public static final Map<String, CompressionType> TYPES_BY_NAME = new HashMap<>();
        public static final String ALLOWED_VALUES;

        static {
            List<String> names = new ArrayList<>();
            for (CompressionType compressionType : CompressionType.values()) {
                TYPES_BY_NAME.put(compressionType.name, compressionType);
                names.add(compressionType.name);
            }
            ALLOWED_VALUES = Utils.join(names, ", ");
        }

        @Override
        public void ensureValid(String name, Object compressionType) {
            String compressionTypeString = ((String) compressionType).trim();
            if (!TYPES_BY_NAME.containsKey(compressionTypeString)) {
                throw new ConfigException(name, compressionType, "Value must be one of: " + ALLOWED_VALUES);
            }
        }

        @Override
        public String toString() {
            return "[" + ALLOWED_VALUES + "]";
        }
    }

}

