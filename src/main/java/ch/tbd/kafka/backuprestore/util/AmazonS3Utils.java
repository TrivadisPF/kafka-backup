package ch.tbd.kafka.backuprestore.util;

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

    public static AmazonS3 initConnection(String region) {
        return initConnection(region, null, -1);
    }

    public static AmazonS3 initConnection(String region, String proxyUrlConfig, int proxyPortConfig) {
        AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();
        builder.withRegion(region);
        builder.setCredentials(new ProfileCredentialsProvider());
        if (proxyUrlConfig != null && !proxyUrlConfig.isEmpty() && proxyPortConfig > 0) {
            ClientConfiguration config = new ClientConfiguration();
            config.setProtocol(Protocol.HTTPS);
            config.setProxyHost(proxyUrlConfig);
            config.setProxyPort(proxyPortConfig);
            builder.withClientConfiguration(config);
        }
        return builder.build();
    }
}
