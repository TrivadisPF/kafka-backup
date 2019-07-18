package ch.tbd.kafka.backuprestore.util;

import ch.tbd.kafka.backuprestore.common.kafkaconnect.AbstractBaseConnectorConfig;
import com.amazonaws.Protocol;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Locale;

import static ch.tbd.kafka.backuprestore.common.kafkaconnect.AbstractBaseConnectorConfig.*;

/**
 * Class S3ProxyConfig.
 * This represents TODO.
 *
 * @author iorfinoa
 * @version $$Revision$$
 */
public class S3ProxyConfig {

    private static final Logger log = LoggerFactory.getLogger(S3ProxyConfig.class);
    private final Protocol protocol;
    private final String host;
    private final int port;
    private final String user;
    private final String pass;

    public S3ProxyConfig(AbstractBaseConnectorConfig connectorConfig) {
        try {
            URL url = new URL(connectorConfig.getString(S3_PROXY_URL_CONFIG));
            protocol = extractProtocol(url.getProtocol());
            host = url.getHost();
            port = url.getPort();
            String username = connectorConfig.getString(S3_PROXY_USER_CONFIG);
            user = StringUtils.isNotBlank(username)
                    ? username
                    : extractUser(url.getUserInfo());
            Password password = connectorConfig.getPassword(S3_PROXY_PASS_CONFIG);
            pass = StringUtils.isNotBlank(password.value())
                    ? password.value()
                    : extractPass(url.getUserInfo());
            log.debug("Using proxy config {}", this);
        } catch (MalformedURLException e) {
            throw new ConfigException(
                    S3_PROXY_URL_CONFIG,
                    connectorConfig.getString(S3_PROXY_URL_CONFIG),
                    e.toString()
            );
        }
    }

    public S3ProxyConfig(String proxyUrl, String proxyUser, Password proxyPass) {
        try {
            URL url = new URL(proxyUrl);
            protocol = extractProtocol(url.getProtocol());
            host = url.getHost();
            port = url.getPort();
            String username = proxyUser;
            user = StringUtils.isNotBlank(username)
                    ? username
                    : extractUser(url.getUserInfo());
            Password password = proxyPass;
            pass = StringUtils.isNotBlank(password.value())
                    ? password.value()
                    : extractPass(url.getUserInfo());
            log.debug("Using proxy config {}", this);
        } catch (MalformedURLException e) {
            throw new ConfigException(
                    S3_PROXY_URL_CONFIG,
                    proxyUrl,
                    e.toString()
            );
        }
    }

    public static Protocol extractProtocol(String protocol) {
        if (StringUtils.isBlank(protocol)) {
            return Protocol.HTTPS;
        }
        return "http".equals(protocol.trim().toLowerCase(Locale.ROOT)) ? Protocol.HTTP : Protocol.HTTPS;
    }

    public static String extractUser(String userInfo) {
        return StringUtils.isBlank(userInfo) ? null : userInfo.split(":")[0];
    }

    public static String extractPass(String userInfo) {
        if (StringUtils.isBlank(userInfo)) {
            return null;
        }

        String[] parts = userInfo.split(":", 2);
        return parts.length == 2 ? parts[1] : null;
    }

    public Protocol protocol() {
        return protocol;
    }

    public String host() {
        return host;
    }

    public int port() {
        return port;
    }

    public String user() {
        return user;
    }

    public String pass() {
        return pass;
    }

    @Override
    public String toString() {
        return "S3ProxyConfig{"
                + "protocol=" + protocol
                + ", host='" + host + '\''
                + ", port=" + port
                + ", user='" + user + '\''
                + ", pass='" + pass + '\''
                + '}';
    }

}
