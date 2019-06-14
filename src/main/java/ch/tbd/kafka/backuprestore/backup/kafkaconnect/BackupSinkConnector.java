package ch.tbd.kafka.backuprestore.backup.kafkaconnect;

import ch.tbd.kafka.backuprestore.util.Version;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BackupSinkConnector extends SinkConnector {

    private static final Logger logger = LoggerFactory.getLogger(BackupSinkConnector.class);
    private BackupSinkConnectorConfig config;

    public BackupSinkConnector() {

    }

    @Override
    public void initialize(ConnectorContext ctx, List<Map<String, String>> taskConfigs) {
        super.initialize(ctx, taskConfigs);
    }

    @Override
    public void reconfigure(Map<String, String> props) {
        super.reconfigure(props);
    }

    // Only for test
    BackupSinkConnector(BackupSinkConnectorConfig config) {
        this.config = config;
    }

    @Override
    public void start(Map<String, String> map) {
        config = new BackupSinkConnectorConfig(map);
        logger.info("Starting backup sink connector {}", config.getName());
    }

    @Override
    public Class<? extends Task> taskClass() {
        return BackupSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        Map<String, String> taskProps = new HashMap<>(config.originalsStrings());
        List<Map<String, String>> taskConfigs = new ArrayList<>(maxTasks);
        for (int i = 0; i < maxTasks; ++i) {
            taskConfigs.add(taskProps);
        }
        return taskConfigs;
    }

    @Override
    public void stop() {

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
