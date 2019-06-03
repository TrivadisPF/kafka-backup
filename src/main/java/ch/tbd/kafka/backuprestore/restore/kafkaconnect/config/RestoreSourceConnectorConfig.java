package ch.tbd.kafka.backuprestore.restore.kafkaconnect.config;

import ch.tbd.kafka.backuprestore.config.ComposableConfig;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.regions.Regions;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.utils.Utils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Class RestoreSourceConnectorConfig.
 * This represents TODO.
 *
 * @author iorfinoa
 * @version $$Revision$$
 */
public class RestoreSourceConnectorConfig extends AbstractConfig implements ComposableConfig {

    public static final String S3_BUCKET_CONFIG = "s3.bucket.name";
    public static final String RESTORE_TOPIC_NAME = "restore.topic";

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


    private String name;


    public RestoreSourceConnectorConfig(Map<String, String> props) {
        this(conf(), props);
    }

    protected RestoreSourceConnectorConfig(ConfigDef conf, Map<String, String> props) {
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

        final String group = "restore-s3";
        int orderInGroup = 0;

        ConfigDef configDef = new ConfigDef()
                .define(
                        S3_BUCKET_CONFIG,
                        ConfigDef.Type.STRING,
                        Importance.HIGH,
                        "The S3 Bucket.",
                        group,
                        ++orderInGroup,
                        Width.LONG,
                        "S3 Bucket"
                );

        configDef.define(
                RESTORE_TOPIC_NAME,
                ConfigDef.Type.STRING,
                Importance.HIGH,
                "The topic name to restore.",
                group,
                ++orderInGroup,
                Width.LONG,
                "The topic name to restore"
        );

        configDef.define(
                S3_PROXY_URL_CONFIG,
                ConfigDef.Type.STRING,
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
                ConfigDef.Type.INT,
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
                ConfigDef.Type.STRING,
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

        return configDef;
    }

    protected static String parseName(Map<String, String> props) {
        String nameProp = props.get("name");
        return nameProp != null ? nameProp : "Restore-sink";
    }

    public String getBucketName() {
        return getString(S3_BUCKET_CONFIG);
    }

    public String getRestoreTopicName() {
        return getString(RESTORE_TOPIC_NAME);
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

}
