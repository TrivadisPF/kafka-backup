package ch.tbd.kafka.backuprestore.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Class Version.
 * This represents TODO.
 *
 * @author iorfinoa
 * @version $$Revision$$
 */
public class Version {
    private static final Logger log = LoggerFactory.getLogger(Version.class);
    private static String versionConnector = "unknown";
    private static final String FILE = "/kafka-backup-restore.properties";

    static {
        try {
            Properties props = new Properties();
            props.load(Version.class.getResourceAsStream(FILE));
            versionConnector = props.getProperty("version", versionConnector).trim();
        } catch (Exception e) {
            log.warn("Error while loading version:" + FILE, e);
        }
    }

    public static String getVersion() {
        return versionConnector;
    }

}
