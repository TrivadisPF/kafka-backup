package ch.tbd.kafka.backuprestore.backup.kafkaconnect.consumer_offsets;

import ch.tbd.kafka.backuprestore.backup.kafkaconnect.AbstractBackupSinkConnector;
import ch.tbd.kafka.backuprestore.backup.kafkaconnect.config.BackupSinkConnectorConfig;
import ch.tbd.kafka.backuprestore.backup.kafkaconnect.consumer_offsets.config.ConsumerOffsetsBackupSinkConnectorConfig;
import ch.tbd.kafka.backuprestore.util.Version;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class ConsumerOffsetsBackupSinkConnector extends AbstractBackupSinkConnector {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerOffsetsBackupSinkConnector.class);
    private ConsumerOffsetsBackupSinkConnectorConfig config;

    @Override
    public void start(Map<String, String> map) {
        this.config = new ConsumerOffsetsBackupSinkConnectorConfig(map);
        List<String> topics = config.getTopicsList();
        super.initAmazonS3(this.config);
        for (String topic : topics) {
            super.initRetention(this.config, topic);
        }
        logger.info("Starting backup sink connector {}", this.config.getName());
    }

    @Override
    public Class<? extends Task> taskClass() {
        return ConsumerOffsetsBackupSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        return super.initTaskConfig(this.config, maxTasks);
    }

    @Override
    public void stop() {
        logger.info("Stop BackupSinkConnector");
    }

    @Override
    public ConfigDef config() {
        return BackupSinkConnectorConfig.conf();
    }

    @Override
    public String version() {
        return Version.getVersion();
    }
}
