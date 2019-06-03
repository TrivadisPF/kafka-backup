package ch.tbd.kafka.backuprestore.restore.kafkaconnect;

import ch.tbd.kafka.backuprestore.restore.kafkaconnect.config.RestoreSourceConnectorConfig;
import ch.tbd.kafka.backuprestore.util.AmazonS3Utils;
import ch.tbd.kafka.backuprestore.util.Version;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.*;

public class RestoreSourceConnector extends SourceConnector {

    private RestoreSourceConnectorConfig connectorConfig;
    private AmazonS3 amazonS3;

    @Override
    public void start(Map<String, String> map) {
        connectorConfig = new RestoreSourceConnectorConfig(map);
        amazonS3 = AmazonS3Utils.initConnection(connectorConfig.getRegionConfig(),
                connectorConfig.getProxyUrlConfig(), connectorConfig.getProxyPortConfig());
    }

    @Override
    public Class<? extends Task> taskClass() {
        return RestoreSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {

        ListObjectsV2Request req = new ListObjectsV2Request().
                withBucketName(connectorConfig.getBucketName()).withPrefix(connectorConfig.getRestoreTopicName() + AmazonS3Utils.SEPARATOR);
        ListObjectsV2Result result = amazonS3.listObjectsV2(req);

        List<S3ObjectSummary> s3ObjectSummaries = result.getObjectSummaries();
        Set<Integer> partitionsSet = new HashSet<>();

        s3ObjectSummaries.stream().forEach(s3ObjectSummary -> {
            String[] keys = s3ObjectSummary.getKey().split("/");
            int partition = Integer.parseInt(keys[1]);
            partitionsSet.add(partition);
        });


        Integer[] partitions = partitionsSet.toArray(new Integer[partitionsSet.size()]);

        int index = 0;

        Map<String, String> taskProps = new HashMap<>(connectorConfig.originalsStrings());
        List<Map<String, String>> taskConfigs = new ArrayList<>(maxTasks);
        for (int i = 0; i < maxTasks; ++i) {
            int numPartitionStored = partitionsSet.size();
            int mod = numPartitionStored % maxTasks;

            String partitionsTask = "";

            int lastIndex = (i + 1) * (numPartitionStored / maxTasks);

            if (mod == 0) {
                for (int j = index; j < lastIndex; j++) {
                    partitionsTask += partitions[j];
                    if ((j + 1) < index) {
                        partitionsTask += ";";
                    }
                }
                index = lastIndex;
            } else {
                if ((lastIndex + mod) == numPartitionStored) {
                    lastIndex = numPartitionStored;
                }
                for (int j = index; j < lastIndex; j++) {
                    partitionsTask += partitions[j];
                    if ((j + 1) < lastIndex) {
                        partitionsTask += ";";
                    }
                }
                index = lastIndex;
            }

            taskProps.put("PARTITION_ASSIGNED", partitionsTask);
            taskConfigs.add(taskProps);
        }
        return taskConfigs;
    }

    //Validate configuration
    @Override
    public Config validate(Map<String, String> connectorConfigs) {
        return super.validate(connectorConfigs);
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

    //ONLY FOR TEST
    public void setAmazonS3(AmazonS3 amazonS3) {
        this.amazonS3 = amazonS3;
    }
}
