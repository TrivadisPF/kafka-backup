package ch.tbd.kafka.backuprestore.backup.kafkaconnect.compact;

import ch.tbd.kafka.backuprestore.backup.kafkaconnect.AbstractBackupSinkConnector;
import ch.tbd.kafka.backuprestore.backup.kafkaconnect.compact.config.CompactBackupSinkConnectorConfig;
import ch.tbd.kafka.backuprestore.util.Version;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Class CompactBackupSinkConnector.
 * This represents TODO.
 *
 * @author iorfinoa
 * @version $$Revision$$
 */
public class CompactBackupSinkConnector extends AbstractBackupSinkConnector {

    private static final Logger logger = LoggerFactory.getLogger(CompactBackupSinkConnector.class);
    private CompactBackupSinkConnectorConfig config;

    @Override
    public void start(Map<String, String> props) {
        config = new CompactBackupSinkConnectorConfig(props);
        List<String> topics = config.getTopicsList();
        super.initAmazonS3(this.config);
        for (String topic : topics) {
            super.initRetention(this.config, topic);
            //super.initializeS3Bucket(this.config, topic);
        }
        logger.info("Starting compact backup sink connector {}", config.getName());
    }

    @Override
    public Class<? extends Task> taskClass() {
        return CompactBackupSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        return super.initTaskConfig(this.config, maxTasks);
    }

    @Override
    public void stop() {
        logger.info("Stop CompactBackupSinkConnector");
    }

    @Override
    public ConfigDef config() {
        return CompactBackupSinkConnectorConfig.conf();
    }

    @Override
    public String version() {
        return Version.getVersion();
    }
}
