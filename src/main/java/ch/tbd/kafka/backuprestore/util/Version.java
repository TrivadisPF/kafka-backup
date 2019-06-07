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
    private static String VERSION = "unknown";
    private static final String file = "/kafka-backup-restore.properties";

    static {
        try {
            Properties props = new Properties();
            props.load(Version.class.getResourceAsStream(file));
            VERSION = props.getProperty("version", VERSION).trim();
        } catch (Exception e) {
            log.warn("Error while loading version:" + file, e);
        }
        log.info(VERSION);
    }

    public static String getVersion() {
        return VERSION;
    }

}
