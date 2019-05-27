package ch.tbd.kafka.backuprestore.backup.kafkaconnect;

import ch.tbd.kafka.backuprestore.config.ComposableConfig;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.model.SSEAlgorithm;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.utils.Utils;

import java.util.*;

import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;

/**
 * Class BackupSinkConnectorConfig.
 * This represents TODO.
 *
 * @author iorfinoa
 * @version $$Revision$$
 */
public class BackupSinkConnectorConfig extends AbstractConfig implements ComposableConfig {

    // S3 Group
    public static final String S3_BUCKET_CONFIG = "s3.bucket.name";

    public static final String S3_PROXY_URL_CONFIG = "s3.proxy.url";
    public static final String S3_PROXY_URL_DEFAULT = "";

    public static final String S3_PROXY_PORT_CONFIG = "s3.proxy.port";
    public static final int S3_PROXY_PORT_DEFAULT = 0;

    public static final String S3_PROXY_USER_CONFIG = "s3.proxy.user";
    public static final String S3_PROXY_USER_DEFAULT = null;

    public static final String S3_PROXY_PASS_CONFIG = "s3.proxy.password";
    public static final Password S3_PROXY_PASS_DEFAULT = new Password(null);

    public static final String REGION_CONFIG = "s3.region";
    public static final String REGION_DEFAULT = Regions.DEFAULT_REGION.getName();

    public static final String S3_RETRY_BACKOFF_CONFIG = "s3.retry.backoff.ms";
    public static final int S3_RETRY_BACKOFF_DEFAULT = 200;

    public static final String FLUSH_SIZE_CONFIG = "flush.size";
    public static final String FLUSH_SIZE_DOC =
            "Number of records written to store before invoking file commits.";
    public static final String FLUSH_SIZE_DISPLAY = "Flush Size";

    public static final String ROTATE_INTERVAL_MS_CONFIG = "rotate.interval.ms";
    public static final String
            ROTATE_INTERVAL_MS_DOC =
            "The time interval in milliseconds to invoke file commits. This configuration ensures that "
                    + "file commits are invoked every configured interval. This configuration is useful when "
                    + "data ingestion rate is low and the connector didn't write enough messages to commit "
                    + "files. The default value -1 means that this feature is disabled.";
    public static final long ROTATE_INTERVAL_MS_DEFAULT = -1L;
    public static final String ROTATE_INTERVAL_MS_DISPLAY = "Rotate Interval (ms)";

    public static final String RETRY_BACKOFF_CONFIG = "retry.backoff.ms";
    public static final String
            RETRY_BACKOFF_DOC =
            "The retry backoff in milliseconds. This config is used to notify Kafka connect to retry "
                    + "delivering a message batch or performing recovery in case of transient exceptions.";
    public static final long RETRY_BACKOFF_DEFAULT = 5000L;
    public static final String RETRY_BACKOFF_DISPLAY = "Retry Backoff (ms)";

    // Schema group
    public static final String SCHEMA_COMPATIBILITY_CONFIG = "schema.compatibility";
    public static final String SCHEMA_COMPATIBILITY_DOC =
            "The schema compatibility rule to use when the connector is observing schema changes. The "
                    + "supported configurations are NONE, BACKWARD, FORWARD and FULL.";
    public static final String SCHEMA_COMPATIBILITY_DEFAULT = "NONE";
    public static final String SCHEMA_COMPATIBILITY_DISPLAY = "Schema Compatibility";

    // CHECKSTYLE:OFF
    public static final ConfigDef.Recommender schemaCompatibilityRecommender =
            new SchemaCompatibilityRecommender();
    // CHECKSTYLE:ON


    public static final String FILENAME_OFFSET_ZERO_PAD_WIDTH_CONFIG =
            "filename.offset.zero.pad.width";
    public static final String
            FILENAME_OFFSET_ZERO_PAD_WIDTH_DOC =
            "Width to zero-pad offsets in store's filenames if offsets are too short in order to "
                    + "provide fixed-width filenames that can be ordered by simple lexicographic sorting.";
    public static final int FILENAME_OFFSET_ZERO_PAD_WIDTH_DEFAULT = 10;
    public static final String FILENAME_OFFSET_ZERO_PAD_WIDTH_DISPLAY =
            "Filename Offset Zero Pad Width";


    private final String name;


    public BackupSinkConnectorConfig(Map<String, String> props) {
        this(conf(), props);
    }

    protected BackupSinkConnectorConfig(ConfigDef conf, Map<String, String> props) {
        super(conf, props);
        this.name = parseName(originalsStrings());
    }

    @Override
    public Object get(String key) {
        return super.get(key);
    }

    public String getName() {
        return name;
    }

    public static ConfigDef conf() {
        final String group = "backup-s3";
        int orderInGroup = 0;

        ConfigDef configDef = new ConfigDef()
                .define(
                        S3_BUCKET_CONFIG,
                        Type.STRING,
                        Importance.HIGH,
                        "The S3 Bucket.",
                        group,
                        ++orderInGroup,
                        Width.LONG,
                        "S3 Bucket"
                );


        List<String> validSsea = new ArrayList<>(SSEAlgorithm.values().length + 1);
        validSsea.add("");
        for (SSEAlgorithm algo : SSEAlgorithm.values()) {
            validSsea.add(algo.toString());
        }

        configDef.define(
                S3_PROXY_URL_CONFIG,
                Type.STRING,
                S3_PROXY_URL_DEFAULT,
                Importance.LOW,
                "S3 Proxy settings encoded in URL syntax. This property is meant to be used only if you"
                        + " need to access S3 through a proxy.",
                group,
                ++orderInGroup,
                Width.LONG,
                "S3 Proxy Settings"
        );

        configDef.define(
                S3_PROXY_PORT_CONFIG,
                Type.INT,
                S3_PROXY_PORT_DEFAULT,
                Importance.LOW,
                "S3 Proxy settings encoded in URL syntax. This property is meant to be used only if you"
                        + " need to access S3 through a proxy.",
                group,
                ++orderInGroup,
                Width.LONG,
                "S3 Proxy Settings"
        );

        configDef.define(
                S3_PROXY_USER_CONFIG,
                Type.STRING,
                S3_PROXY_USER_DEFAULT,
                Importance.LOW,
                "S3 Proxy User. This property is meant to be used only if you"
                        + " need to access S3 through a proxy. Using ``"
                        + S3_PROXY_USER_CONFIG
                        + "`` instead of embedding the username and password in ``"
                        + S3_PROXY_URL_CONFIG
                        + "`` allows the password to be hidden in the logs.",
                group,
                ++orderInGroup,
                Width.LONG,
                "S3 Proxy User"
        );

        configDef.define(
                S3_PROXY_PASS_CONFIG,
                Type.PASSWORD,
                S3_PROXY_PASS_DEFAULT,
                Importance.LOW,
                "S3 Proxy Password. This property is meant to be used only if you"
                        + " need to access S3 through a proxy. Using ``"
                        + S3_PROXY_PASS_CONFIG
                        + "`` instead of embedding the username and password in ``"
                        + S3_PROXY_URL_CONFIG
                        + "`` allows the password to be hidden in the logs.",
                group,
                ++orderInGroup,
                Width.LONG,
                "S3 Proxy Password"
        );

        configDef.define(
                REGION_CONFIG,
                Type.STRING,
                REGION_DEFAULT,
                new RegionValidator(),
                Importance.MEDIUM,
                "The AWS region to be used the connector.",
                group,
                ++orderInGroup,
                Width.LONG,
                "AWS region",
                new RegionRecommender()
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
                "Retry Backoff (ms)"
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

        {
            // Define Schema configuration group
            final String groupSchema = "Schema";
            int orderInGroupSchema = 0;

            // Define Schema configuration group
            configDef.define(
                    SCHEMA_COMPATIBILITY_CONFIG,
                    Type.STRING,
                    SCHEMA_COMPATIBILITY_DEFAULT,
                    Importance.HIGH,
                    SCHEMA_COMPATIBILITY_DOC,
                    groupSchema,
                    ++orderInGroupSchema,
                    Width.SHORT,
                    SCHEMA_COMPATIBILITY_DISPLAY,
                    schemaCompatibilityRecommender
            );
        }

        return configDef;
    }

    protected static String parseName(Map<String, String> props) {
        String nameProp = props.get("name");
        return nameProp != null ? nameProp : "Backup-sink";
    }

    public String getBucketName() {
        return getString(S3_BUCKET_CONFIG);
    }

    public String getProxyUrlConfig() {
        return getString(S3_PROXY_URL_CONFIG);
    }

    public int getProxyPortConfig() {
        return getInt(S3_PROXY_PORT_CONFIG);
    }

    public String getRegionConfig() {
        return getString(REGION_CONFIG);
    }


    private static class RegionRecommender implements ConfigDef.Recommender {
        @Override
        public List<Object> validValues(String name, Map<String, Object> connectorConfigs) {
            return Arrays.<Object>asList(RegionUtils.getRegions());
        }

        @Override
        public boolean visible(String name, Map<String, Object> connectorConfigs) {
            return true;
        }
    }

    private static class RegionValidator implements ConfigDef.Validator {
        @Override
        public void ensureValid(String name, Object region) {
            String regionStr = ((String) region).toLowerCase().trim();
            if (RegionUtils.getRegion(regionStr) == null) {
                throw new ConfigException(
                        name,
                        region,
                        "Value must be one of: " + Utils.join(RegionUtils.getRegions(), ", ")
                );
            }
        }

        @Override
        public String toString() {
            return "[" + Utils.join(RegionUtils.getRegions(), ", ") + "]";
        }
    }


    public static class SchemaCompatibilityRecommender extends BooleanParentRecommender {

        public SchemaCompatibilityRecommender() {
            super("hive.integration");
        }

        @Override
        public List<Object> validValues(String name, Map<String, Object> connectorConfigs) {
            Boolean hiveIntegration = (Boolean) connectorConfigs.get(parentConfigName);
            if (hiveIntegration != null && hiveIntegration) {
                return Arrays.asList("BACKWARD", "FORWARD", "FULL");
            } else {
                return Arrays.asList("NONE", "BACKWARD", "FORWARD", "FULL");
            }
        }

        @Override
        public boolean visible(String name, Map<String, Object> connectorConfigs) {
            return true;
        }
    }

    public static class BooleanParentRecommender implements ConfigDef.Recommender {

        protected final String parentConfigName;

        public BooleanParentRecommender(String parentConfigName) {
            this.parentConfigName = parentConfigName;
        }

        @Override
        public List<Object> validValues(String name, Map<String, Object> connectorConfigs) {
            return new LinkedList<>();
        }

        @Override
        public boolean visible(String name, Map<String, Object> connectorConfigs) {
            return (boolean) connectorConfigs.get(parentConfigName);
        }
    }

}

