package ch.tbd.kafka.backuprestore;

import ch.tbd.kafka.backuprestore.common.kafkaconnect.AbstractBaseConnectorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * Class AbstractTest.
 * This represents TODO.
 *
 * @author iorfinoa
 * @version $$Revision$$
 */
public abstract class AbstractTest {

    private Logger logger = LoggerFactory.getLogger(AbstractTest.class);
    private Properties props;
    protected final String PREFIX_KEY = "prefix.folder";

    public AbstractTest() {
        try {
            props = new Properties();
            props.load(this.getClass().getResourceAsStream("/application-test.properties"));
            if (getListPropertyFiles() != null) {
                for (String file : getListPropertyFiles()) {
                    props.load(this.getClass().getResourceAsStream(file));
                }
            }
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
    }

    protected void addProperty(String key, String value) {
        props.setProperty(key, value);
    }

    protected Map<String, String> getPropertiesMap() {
        Map<String, String> configurationMap = new HashMap<>();
        Iterator<Object> it = props.keySet().iterator();
        while (it.hasNext()) {
            String key = (String) it.next();
            configurationMap.put(key, props.getProperty(key));
        }
        return configurationMap;
    }

    protected String getBucketName() {
        return getPropertiesMap().get(AbstractBaseConnectorConfig.S3_BUCKET_CONFIG);
    }

    /**
     * Please provide specific file (it needed) in the following format '/class-name.properties'
     *
     * @return
     */
    protected abstract List<String> getListPropertyFiles();
}
