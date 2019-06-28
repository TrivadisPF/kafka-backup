package ch.tbd.kafka.backuprestore.backup.kafkaconnect.config;

import ch.tbd.kafka.backuprestore.backup.storage.CompressionType;
import ch.tbd.kafka.backuprestore.common.kafkaconnect.AbstractBaseConnectorConfig;
import com.amazonaws.ClientConfiguration;
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

import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;

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

    public static final String S3_PART_RETRIES_CONFIG = "s3.part.retries";
    public static final int S3_PART_RETRIES_DEFAULT = 3;

    public static final String S3_RETRY_BACKOFF_CONFIG = "s3.retry.backoff.ms";
    private static final int S3_RETRY_BACKOFF_DEFAULT = 200;
    private static final String S3_RETRY_BACKOFF_DISPLAY = "Retry Backoff (ms)";

    public static final String HEADERS_USE_EXPECT_CONTINUE_CONFIG =
            "s3.http.send.expect.continue";
    public static final boolean HEADERS_USE_EXPECT_CONTINUE_DEFAULT =
            ClientConfiguration.DEFAULT_USE_EXPECT_CONTINUE;

    private static final String FLUSH_SIZE_CONFIG = "flush.size";
    private static final String FLUSH_SIZE_DOC =
            "Number of records written to store before invoking file commits.";
    private static final String FLUSH_SIZE_DISPLAY = "Flush Size";

    private static final String ROTATE_INTERVAL_MS_CONFIG = "rotate.interval.ms";
    private static final String
            ROTATE_INTERVAL_MS_DOC =
            "The time interval in milliseconds to invoke file commits. This configuration ensures that "
                    + "file commits are invoked every configured interval. This configuration is useful when "
                    + "data ingestion rate is low and the connector didn't write enough messages to commit "
                    + "files. The default value -1 means that this feature is disabled.";
    private static final long ROTATE_INTERVAL_MS_DEFAULT = -1L;
    private static final String ROTATE_INTERVAL_MS_DISPLAY = "Rotate Interval (ms)";

    private static final String RETRY_BACKOFF_CONFIG = "retry.backoff.ms";
    private static final String
            RETRY_BACKOFF_DOC =
            "The retry backoff in milliseconds. This config is used to notify Kafka connect to retry "
                    + "delivering a message batch or performing recovery in case of transient exceptions.";
    private static final long RETRY_BACKOFF_DEFAULT = 5000L;
    private static final String RETRY_BACKOFF_DISPLAY = S3_RETRY_BACKOFF_DISPLAY;

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
                S3_PART_RETRIES_CONFIG,
                Type.INT,
                S3_PART_RETRIES_DEFAULT,
                atLeast(0),
                Importance.MEDIUM,
                "Maximum number of retry attempts for failed requests. Zero means no retries. "
                        + "The actual number of attempts is determined by the S3 client based on multiple "
                        + "factors, including, but not limited to - "
                        + "the value of this parameter, type of exception occurred, "
                        + "throttling settings of the underlying S3 client, etc.",
                group,
                ++orderInGroup,
                Width.LONG,
                "S3 Part Upload Retries"
        );

        configDef.define(
                S3_RETRY_BACKOFF_CONFIG,
                Type.LONG,
                S3_RETRY_BACKOFF_DEFAULT,
                atLeast(0L),
                Importance.LOW,
                "How long to wait in milliseconds before attempting the first retry "
                        + "of a failed S3 request. Upon a failure, this connector may wait up to twice as "
                        + "long as the previous wait, up to the maximum number of retries. "
                        + "This avoids retrying in a tight loop under failure scenarios.",
                group,
                ++orderInGroup,
                Width.SHORT,
                S3_RETRY_BACKOFF_DISPLAY
        );

        configDef.define(
                HEADERS_USE_EXPECT_CONTINUE_CONFIG,
                Type.BOOLEAN,
                HEADERS_USE_EXPECT_CONTINUE_DEFAULT,
                Importance.LOW,
                "Enable/disable use of the HTTP/1.1 handshake using EXPECT: 100-CONTINUE during "
                        + "multi-part upload. If true, the client will wait for a 100 (CONTINUE) response "
                        + "before sending the request body. Else, the client uploads the entire request "
                        + "body without checking if the server is willing to accept the request.",
                group,
                ++orderInGroup,
                Width.SHORT,
                "S3 HTTP Send Uses Expect Continue"
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
                ROTATE_INTERVAL_MS_CONFIG,
                Type.LONG,
                ROTATE_INTERVAL_MS_DEFAULT,
                Importance.HIGH,
                ROTATE_INTERVAL_MS_DOC,
                group,
                ++orderInGroup,
                Width.MEDIUM,
                ROTATE_INTERVAL_MS_DISPLAY
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

    public long getRotateIntervalMs() {
        return getLong(ROTATE_INTERVAL_MS_CONFIG);
    }

    public long getRetryBackoffDefault() {
        return getLong(RETRY_BACKOFF_CONFIG);
    }

    public int getS3PartRetries() {
        return getInt(S3_PART_RETRIES_CONFIG);
    }

    public boolean useExpectContinue() {
        return getBoolean(HEADERS_USE_EXPECT_CONTINUE_CONFIG);
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

