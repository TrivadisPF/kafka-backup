package ch.tbd.kafka.backuprestore.backup.kafkaconnect;

import ch.tbd.kafka.backuprestore.backup.kafkaconnect.compact.config.CompactBackupSinkConnectorConfig;
import ch.tbd.kafka.backuprestore.backup.kafkaconnect.config.BackupSinkConnectorConfig;
import ch.tbd.kafka.backuprestore.util.AmazonS3Utils;
import ch.tbd.kafka.backuprestore.util.Constants;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.BucketLifecycleConfiguration;
import com.amazonaws.services.s3.model.lifecycle.LifecycleFilter;
import com.amazonaws.services.s3.model.lifecycle.LifecyclePrefixPredicate;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Class AbstractBackupSinkConnector.
 * This represents TODO.
 *
 * @author iorfinoa
 * @version $$Revision$$
 */
public abstract class AbstractBackupSinkConnector extends SinkConnector {

    private Logger logger = LoggerFactory.getLogger(AbstractBackupSinkConnector.class);

    protected AmazonS3 amazonS3;

    protected void initAmazonS3(BackupSinkConnectorConfig config) {
        amazonS3 = AmazonS3Utils.initConnection(config);
    }

    protected void initRetention(BackupSinkConnectorConfig config, String topic) {
        boolean exit = false;
        while (!exit) {
            StringBuilder idRuleRetentionRuleId = new StringBuilder(config.getName());
            idRuleRetentionRuleId.append("-rule-");
            idRuleRetentionRuleId.append(topic);
            StringBuilder prefixRuleId = new StringBuilder(topic);
            prefixRuleId.append(Constants.S3_KEY_SEPARATOR);
            if (config instanceof CompactBackupSinkConnectorConfig) {
                prefixRuleId.append(config.getName());
                prefixRuleId.append(Constants.S3_KEY_SEPARATOR);
            }
            List<BucketLifecycleConfiguration.Rule> rules = new ArrayList<>();

            BucketLifecycleConfiguration.Rule rule1 = new BucketLifecycleConfiguration.Rule()
                    .withId(idRuleRetentionRuleId.toString())
                    .withFilter(new LifecycleFilter((new LifecyclePrefixPredicate(prefixRuleId.toString()))))
                    .withExpirationInDays(config.getS3RetentionInDays())
                    .withStatus(BucketLifecycleConfiguration.ENABLED);

            BucketLifecycleConfiguration configuration = amazonS3.getBucketLifecycleConfiguration(config.getBucketName());
            rules.add(rule1);
            if (configuration != null) {
                if (configuration.getRules() != null) {
                    for (BucketLifecycleConfiguration.Rule ruleTmp : configuration.getRules()) {
                        if (!ruleTmp.getId().equalsIgnoreCase(idRuleRetentionRuleId.toString())) {
                            rules.add(ruleTmp);
                        }
                    }
                }
            } else {
                configuration = new BucketLifecycleConfiguration();
            }
            configuration.setRules(rules);
            try {
                amazonS3.setBucketLifecycleConfiguration(config.getBucketName(), configuration);
                exit = true;
            } catch (AmazonServiceException e) {
                logger.error(e.getMessage(), e);
            } catch (SdkClientException e) {
                logger.error(e.getMessage(), e);
            }
        }
    }

    protected List<Map<String, String>> initTaskConfig(BackupSinkConnectorConfig config, int maxTasks) {
        Map<String, String> taskProps = new HashMap<>(config.originalsStrings());
        List<Map<String, String>> taskConfigs = new ArrayList<>(maxTasks);
        for (int i = 0; i < maxTasks; ++i) {
            taskConfigs.add(taskProps);
        }
        return taskConfigs;
    }

}
