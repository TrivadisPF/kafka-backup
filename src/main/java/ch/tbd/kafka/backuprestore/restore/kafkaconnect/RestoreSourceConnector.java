package ch.tbd.kafka.backuprestore.restore.kafkaconnect;

import ch.tbd.kafka.backuprestore.restore.kafkaconnect.config.RestoreSourceConnectorConfig;
import ch.tbd.kafka.backuprestore.util.Version;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RestoreSourceConnector extends SourceConnector {

    private RestoreSourceConnectorConfig connectorConfig;

    @Override
    public void start(Map<String, String> map) {
        connectorConfig = new RestoreSourceConnectorConfig(map);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return RestoreSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        Map<String, String> taskProps = new HashMap<>(connectorConfig.originalsStrings());
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
        return RestoreSourceConnectorConfig.conf();
    }

    @Override
    public String version() {
        return Version.getVersion();
    }
}
