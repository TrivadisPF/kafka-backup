package ch.tbd.kafka.backuprestore.util;

import ch.tbd.kafka.backuprestore.common.kafkaconnect.AbstractBaseConnectorConfig;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

/**
 * Class AmazonS3Utils.
 * This represents TODO.
 *
 * @author iorfinoa
 * @version $$Revision$$
 */
public class AmazonS3Utils {

    public static final String SEPARATOR = "/";

    public static AmazonS3 initConnection(AbstractBaseConnectorConfig connectorConfig) {
        AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();
        builder.withRegion(connectorConfig.getRegionConfig());
        builder.setCredentials(new ProfileCredentialsProvider());
        if (connectorConfig.getProxyUrlConfig() != null && !connectorConfig.getProxyUrlConfig().isEmpty() && connectorConfig.getProxyPortConfig() > 0) {
            ClientConfiguration config = new ClientConfiguration();
            config.setProtocol(Protocol.HTTPS);
            config.setProxyHost(connectorConfig.getProxyUrlConfig());
            config.setProxyPort(connectorConfig.getProxyPortConfig());
            builder.withClientConfiguration(config);
        }
        return builder.build();
    }
}
