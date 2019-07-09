package ch.tbd.kafka.backuprestore.common.kafkaconnect;

import ch.tbd.kafka.backuprestore.config.ComposableConfig;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.SSEAlgorithm;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Class AbstractBaseConnectorConfig.
 * This represents TODO.
 *
 * @author iorfinoa
 * @version $$Revision$$
 */
public abstract class AbstractBaseConnectorConfig extends AbstractConfig implements ComposableConfig {

    private static Logger logger = LoggerFactory.getLogger(AbstractBaseConnectorConfig.class);
    public static final String S3_BUCKET_CONFIG = "s3.bucket.name";
    private static final String S3_BUCKET_DOC = "The S3 Bucket.";
    private static final String S3_BUCKET_DISPLAY = "S3 Bucket";

    public static final String S3_PROFILE_NAME_CONFIG = "s3.profile.name";
    private static final String S3_PROFILE_NAME_DOC = "The profile name to use in Amazon configuration.";
    private static final String S3_PROFILE_NAME_DEFAULT = null;
    private static final String S3_PROFILE_NAME_DISPLAY = "S3 Profile name";

    private static final String SSEA_CONFIG = "s3.ssea.name";
    private static final String SSEA_CONFIG_DOC = "The S3 Server Side Encryption Algorithm.";
    private static final String SSEA_CONFIG_DISPLAY = "S3 Server Side Encryption Algorithm";
    private static final String SSEA_DEFAULT = "";

    private static final String SSE_CUSTOMER_KEY = "s3.sse.customer.key";
    private static final String SSE_CUSTOMER_KEY_DOC = "The S3 Server Side Encryption Customer-Provided Key (SSE-C).";
    private static final Password SSE_CUSTOMER_KEY_DEFAULT = new Password(null);
    private static final String SSE_CUSTOMER_KEY_DISPLAY = "S3 Server Side Encryption Customer-Provided Key (SSE-C)";

    private static final String SSE_KMS_KEY_ID_CONFIG = "s3.sse.kms.key.id";
    private static final String SSE_KMS_KEY_ID_DOC = "The name of the AWS Key Management Service (AWS-KMS) key to be used for server side "
            + "encryption of the S3 objects. No encryption is used when no key is provided, but"
            + " it is enabled when '" + SSEAlgorithm.KMS + "' is specified as encryption "
            + "algorithm with a valid key name.";
    private static final String SSE_KMS_KEY_ID_DEFAULT = "";
    private static final String SSE_KMS_KEY_DISPLAY = "S3 Server Side Encryption Key";

    private static final String ACL_CANNED_CONFIG = "s3.acl.canned";
    private static final String ACL_CANNED_DOC = "An S3 canned ACL header value to apply when writing objects.";
    private static final String ACL_CANNED_DEFAULT = null;
    private static final String ACL_CANNED_DISPLAY = "S3 Canned ACL";

    public static final String S3_PROXY_URL_CONFIG = "s3.proxy.url";
    private static final String S3_PROXY_URL_DOC = "S3 Proxy settings encoded in URL syntax. This property is meant to be used only if you"
            + " need to access S3 through a proxy.";
    private static final String S3_PROXY_URL_DEFAULT = "";
    private static final String S3_PROXY_URL_DISPLAY = "S3 Proxy URL Settings";

    public static final String S3_PROXY_PORT_CONFIG = "s3.proxy.port";
    private static final String S3_PROXY_PORT_DOC = "S3 Proxy settings encoded in URL syntax. This property is meant to be used only if you"
            + " need to access S3 through a proxy.";
    private static final int S3_PROXY_PORT_DEFAULT = 0;
    private static final String S3_PROXY_PORT_DISPLAY = "S3 Proxy Port Settings";

    public static final String S3_PROXY_USER_CONFIG = "s3.proxy.user";
    private static final String S3_PROXY_USER_DOC = "S3 Proxy User. This property is meant to be used only if you"
            + " need to access S3 through a proxy. Using ``"
            + S3_PROXY_USER_CONFIG
            + "`` instead of embedding the username and password in ``"
            + S3_PROXY_URL_CONFIG
            + "`` allows the password to be hidden in the logs.";
    private static final String S3_PROXY_USER_DEFAULT = null;
    private static final String S3_PROXY_USER_DISPLAY = "S3 Proxy User";

    public static final String S3_PROXY_PASS_CONFIG = "s3.proxy.password";
    private static final String S3_PROXY_PASS_DOC = "S3 Proxy Password. This property is meant to be used only if you"
            + " need to access S3 through a proxy. Using ``"
            + S3_PROXY_PASS_CONFIG
            + "`` instead of embedding the username and password in ``"
            + S3_PROXY_URL_CONFIG
            + "`` allows the password to be hidden in the logs.";
    private static final Password S3_PROXY_PASS_DEFAULT = new Password(null);
    private static final String S3_PROXY_PASS_DISPLAY = "S3 Proxy Password";

    public static final String S3_REGION_CONFIG = "s3.region";
    private static final String S3_REGION_DOC = "The AWS region to be used the connector.";
    private static final String S3_REGION_DEFAULT = Regions.DEFAULT_REGION.getName();
    private static final String S3_REGION_DISPLAY = "AWS region";

    public static final String WAN_MODE_CONFIG = "s3.wan.mode";
    private static final boolean WAN_MODE_DEFAULT = false;


    protected AbstractBaseConnectorConfig(ConfigDef conf, Map<String, String> props) {
        super(conf, props);

        logger.info("AbstractBaseConnectorConfig(ConfigDef conf, Map<String, String> props)");
    }

    @Override
    public Object get(String key) {
        return super.get(key);
    }

    public static ConfigDef conf() {
        final String group = "abs-config-s3";
        int orderInGroup = 0;

        ConfigDef configDef = new ConfigDef();
        configDef.define(
                S3_BUCKET_CONFIG,
                Type.STRING,
                Importance.HIGH,
                S3_BUCKET_DOC,
                group,
                ++orderInGroup,
                Width.LONG,
                S3_BUCKET_DISPLAY
        );

        configDef.define(
                S3_PROFILE_NAME_CONFIG,
                Type.STRING,
                S3_PROFILE_NAME_DEFAULT,
                Importance.HIGH,
                S3_PROFILE_NAME_DOC,
                group,
                ++orderInGroup,
                Width.LONG,
                S3_PROFILE_NAME_DISPLAY
        );

        List<String> validSsea = new ArrayList<>(SSEAlgorithm.values().length + 1);
        validSsea.add("");
        for (SSEAlgorithm algo : SSEAlgorithm.values()) {
            validSsea.add(algo.toString());
        }
        configDef.define(
                SSEA_CONFIG,
                Type.STRING,
                SSEA_DEFAULT,
                ConfigDef.ValidString.in(validSsea.toArray(new String[validSsea.size()])),
                Importance.LOW,
                SSEA_CONFIG_DOC,
                group,
                ++orderInGroup,
                Width.LONG,
                SSEA_CONFIG_DISPLAY,
                new SseAlgorithmRecommender()
        );

        configDef.define(
                SSE_CUSTOMER_KEY,
                Type.PASSWORD,
                SSE_CUSTOMER_KEY_DEFAULT,
                Importance.LOW,
                SSE_CUSTOMER_KEY_DOC,
                group,
                ++orderInGroup,
                Width.LONG,
                SSE_CUSTOMER_KEY_DISPLAY
        );

        configDef.define(
                SSE_KMS_KEY_ID_CONFIG,
                Type.STRING,
                SSE_KMS_KEY_ID_DEFAULT,
                Importance.LOW,
                SSE_KMS_KEY_ID_DOC,
                group,
                ++orderInGroup,
                Width.LONG,
                SSE_KMS_KEY_DISPLAY,
                new SseKmsKeyIdRecommender()
        );

        configDef.define(
                ACL_CANNED_CONFIG,
                Type.STRING,
                ACL_CANNED_DEFAULT,
                new CannedAclValidator(),
                Importance.LOW,
                ACL_CANNED_DOC,
                group,
                ++orderInGroup,
                Width.LONG,
                ACL_CANNED_DISPLAY
        );


        configDef.define(
                S3_PROXY_URL_CONFIG,
                ConfigDef.Type.STRING,
                S3_PROXY_URL_DEFAULT,
                Importance.LOW,
                S3_PROXY_URL_DOC,
                group,
                ++orderInGroup,
                Width.LONG,
                S3_PROXY_URL_DISPLAY
        );

        configDef.define(
                S3_PROXY_PORT_CONFIG,
                ConfigDef.Type.INT,
                S3_PROXY_PORT_DEFAULT,
                Importance.LOW,
                S3_PROXY_PORT_DOC,
                group,
                ++orderInGroup,
                Width.LONG,
                S3_PROXY_PORT_DISPLAY
        );

        configDef.define(
                S3_PROXY_USER_CONFIG,
                ConfigDef.Type.STRING,
                S3_PROXY_USER_DEFAULT,
                Importance.LOW,
                S3_PROXY_USER_DOC,
                group,
                ++orderInGroup,
                Width.LONG,
                S3_PROXY_USER_DISPLAY
        );

        configDef.define(
                S3_PROXY_PASS_CONFIG,
                Type.PASSWORD,
                S3_PROXY_PASS_DEFAULT,
                Importance.LOW,
                S3_PROXY_PASS_DOC,
                group,
                ++orderInGroup,
                Width.LONG,
                S3_PROXY_PASS_DISPLAY
        );

        configDef.define(
                S3_REGION_CONFIG,
                Type.STRING,
                S3_REGION_DEFAULT,
                new RegionValidator(),
                Importance.MEDIUM,
                S3_REGION_DOC,
                group,
                ++orderInGroup,
                Width.LONG,
                S3_REGION_DISPLAY,
                new RegionRecommender()
        );

        configDef.define(
                WAN_MODE_CONFIG,
                Type.BOOLEAN,
                WAN_MODE_DEFAULT,
                Importance.MEDIUM,
                "Use S3 accelerated endpoint.",
                group,
                ++orderInGroup,
                Width.LONG,
                "S3 accelerated endpoint enabled"
        );

        return configDef;
    }

    public String getBucketName() {
        return getString(S3_BUCKET_CONFIG);
    }

    public String getS3ProfileNameConfig() {
        return getString(S3_PROFILE_NAME_CONFIG);
    }

    public String getSsea() {
        return getString(SSEA_CONFIG);
    }

    public String getSseCustomerKey() {
        return getPassword(SSE_CUSTOMER_KEY).value();
    }

    public String getSseKmsKeyId() {
        return getString(SSE_KMS_KEY_ID_CONFIG);
    }

    public CannedAccessControlList getCannedAcl() {
        return CannedAclValidator.ACLS_BY_HEADER_VALUE.get(getString(ACL_CANNED_CONFIG));
    }

    public String getProxyUrlConfig() {
        return getString(S3_PROXY_URL_CONFIG);
    }

    public int getProxyPortConfig() {
        return getInt(S3_PROXY_PORT_CONFIG);
    }

    public String getRegionConfig() {
        return getString(S3_REGION_CONFIG);
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

    private static class SseAlgorithmRecommender implements ConfigDef.Recommender {
        @Override
        public List<Object> validValues(String name, Map<String, Object> connectorConfigs) {
            List<SSEAlgorithm> list = Arrays.asList(SSEAlgorithm.values());
            return new ArrayList<>(list);
        }

        @Override
        public boolean visible(String name, Map<String, Object> connectorConfigs) {
            return true;
        }
    }

    private static class SseKmsKeyIdRecommender implements ConfigDef.Recommender {

        @Override
        public List<Object> validValues(String name, Map<String, Object> connectorConfigs) {
            return new LinkedList<>();
        }

        @Override
        public boolean visible(String name, Map<String, Object> connectorConfigs) {
            return SSEAlgorithm.KMS.toString()
                    .equalsIgnoreCase((String) connectorConfigs.get(SSEA_CONFIG));
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

    private static class CannedAclValidator implements ConfigDef.Validator {
        protected static final Map<String, CannedAccessControlList> ACLS_BY_HEADER_VALUE = new HashMap<>();
        protected static final String ALLOWED_VALUES;

        static {
            List<String> aclHeaderValues = new ArrayList<>();
            for (CannedAccessControlList acl : CannedAccessControlList.values()) {
                ACLS_BY_HEADER_VALUE.put(acl.toString(), acl);
                aclHeaderValues.add(acl.toString());
            }
            ALLOWED_VALUES = Utils.join(aclHeaderValues, ", ");
        }

        @Override
        public void ensureValid(String name, Object cannedAcl) {
            if (cannedAcl == null) {
                return;
            }
            String aclStr = ((String) cannedAcl).trim();
            if (!ACLS_BY_HEADER_VALUE.containsKey(aclStr)) {
                throw new ConfigException(name, cannedAcl, "Value must be one of: " + ALLOWED_VALUES);
            }
        }

        @Override
        public String toString() {
            return "[" + ALLOWED_VALUES + "]";
        }
    }

}
