package ch.tbd.kafka.backuprestore.restore.kafkaconnect;

import ch.tbd.kafka.backuprestore.restore.kafkaconnect.config.RestoreSourceConnectorConfig;
import ch.tbd.kafka.backuprestore.util.AmazonS3Utils;
import ch.tbd.kafka.backuprestore.util.Constants;
import ch.tbd.kafka.backuprestore.util.Version;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class RestoreSourceConnector extends SourceConnector {

    private Logger logger = LoggerFactory.getLogger(RestoreSourceConnector.class);
    private RestoreSourceConnectorConfig connectorConfig;
    private AmazonS3 amazonS3;

    @Override
    public void start(Map<String, String> map) {
        this.connectorConfig = new RestoreSourceConnectorConfig(map);
        this.amazonS3 = AmazonS3Utils.initConnection(this.connectorConfig);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return RestoreSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        ListObjectsRequest req = new ListObjectsRequest().
                withBucketName(connectorConfig.getBucketName()).withPrefix(connectorConfig.getTopicS3Name() + AmazonS3Utils.SEPARATOR);
        ObjectListing result = amazonS3.listObjects(req);

        List<S3ObjectSummary> s3ObjectSummaries = result.getObjectSummaries();
        while (result.isTruncated()) {
            result = amazonS3.listNextBatchOfObjects(result);
            s3ObjectSummaries.addAll(result.getObjectSummaries());
        }
        Set<Integer> partitionsSet = new HashSet<>();

        s3ObjectSummaries.stream().forEach(s3ObjectSummary -> {
            String[] keys = s3ObjectSummary.getKey().split("/");
            int partition = Integer.parseInt(keys[1]);
            partitionsSet.add(partition);
        });

        Integer[] partitions = partitionsSet.toArray(new Integer[partitionsSet.size()]);

        int index = 0;

        if (maxTasks > partitionsSet.size()) {
            maxTasks = partitionsSet.size();
            logger.warn("Number of task greather than the partitions present inside the backup. " +
                    "The number of task will be the same of the number of partitions found inside the backup {}", partitionsSet.size());
        }

        List<Map<String, String>> taskConfigs = new ArrayList<>(maxTasks);
        for (int i = 0; i < maxTasks; ++i) {
            Map<String, String> taskProps = new HashMap<>(connectorConfig.originalsStrings());
            int numPartitionStored = partitionsSet.size();
            int mod = numPartitionStored % maxTasks;

            String partitionsTask = "";

            int lastIndex = (i + 1) * (numPartitionStored / maxTasks);

            if (mod == 0) {
                for (int j = index; j < lastIndex; j++) {
                    partitionsTask += partitions[j];
                    if ((j + 1) < lastIndex) {
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

            taskProps.put(Constants.PARTITION_ASSIGNED_KEY, partitionsTask);
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
