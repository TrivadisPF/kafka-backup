package ch.tbd.kafka.backuprestore.backup.kafkaconnect;

import ch.tbd.kafka.backuprestore.backup.kafkaconnect.config.BackupSinkConnectorConfig;
import ch.tbd.kafka.backuprestore.util.AmazonS3Utils;
import ch.tbd.kafka.backuprestore.util.Constants;
import ch.tbd.kafka.backuprestore.util.Version;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.BucketLifecycleConfiguration;
import com.amazonaws.services.s3.model.lifecycle.LifecycleFilter;
import com.amazonaws.services.s3.model.lifecycle.LifecyclePrefixPredicate;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class BackupSinkConnector extends SinkConnector {

    private static final Logger logger = LoggerFactory.getLogger(BackupSinkConnector.class);
    private BackupSinkConnectorConfig config;

    @Override
    public void start(Map<String, String> map) {
        config = new BackupSinkConnectorConfig(map);
        String topic = String.valueOf(map.get("topics"));

        String prefix = topic + Constants.KEY_SEPARATOR;
        String idRule = this.config.getName() + "-rule-" + topic;

        AmazonS3 amazonS3 = AmazonS3Utils.initConnection(this.config);
        BucketLifecycleConfiguration.Rule rule1 = new BucketLifecycleConfiguration.Rule()
                .withId(idRule)
                .withFilter(new LifecycleFilter((new LifecyclePrefixPredicate(prefix))))
                .withExpirationInDays(this.config.getS3RetentionInDays())
                .withStatus(BucketLifecycleConfiguration.ENABLED);

        BucketLifecycleConfiguration configuration = new BucketLifecycleConfiguration()
                .withRules(Arrays.asList(rule1));
        amazonS3.setBucketLifecycleConfiguration(this.config.getBucketName(), configuration);

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
