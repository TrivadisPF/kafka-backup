package ch.tbd.kafka.backuprestore.restore.kafkaconnect;

import ch.tbd.kafka.backuprestore.model.PartitionRecord;
import ch.tbd.kafka.backuprestore.model.RestoreTopicName;
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
import java.util.stream.Collectors;

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
        List<RestoreTopicName> listTopicNames = connectorConfig.getTopicName();
        Map<String, PartitionRecord> mapTopicPartitionRecord = new HashMap<>();
        int countTotalPartitions = 0;
        for (RestoreTopicName restoreTopicName : listTopicNames) {
            final int positionToSplit;
            ListObjectsRequest req = null;
            if (this.connectorConfig.isInstanceNameToRestoreConfigDefined()) {
                req = new ListObjectsRequest().
                        withBucketName(connectorConfig.getBucketName())
                        .withPrefix(restoreTopicName.getS3TopicName() + Constants.S3_KEY_SEPARATOR + this.connectorConfig.getInstanceNameToRestoreConfig() + Constants.S3_KEY_SEPARATOR);
                positionToSplit = 2;
            } else {
                req = new ListObjectsRequest().
                        withBucketName(connectorConfig.getBucketName()).withPrefix(restoreTopicName.getS3TopicName() + Constants.S3_KEY_SEPARATOR);
                positionToSplit = 1;
            }

            ObjectListing result = amazonS3.listObjects(req);

            List<S3ObjectSummary> s3ObjectSummaries = result.getObjectSummaries();
            while (result.isTruncated()) {
                result = amazonS3.listNextBatchOfObjects(result);
                s3ObjectSummaries.addAll(result.getObjectSummaries());
            }
            if (this.connectorConfig.isInstanceNameToRestoreConfigDefined()) {
                for (S3ObjectSummary s3ObjectSummary : s3ObjectSummaries) {
                    int len = s3ObjectSummary.getKey().split(Constants.S3_KEY_SEPARATOR).length;
                    if (len != 4) {
                        logger.error("Is defined an instance {} but no backup it was found.", this.connectorConfig.getInstanceNameToRestoreConfig());
                        stop();
                        return Collections.emptyList();
                    }
                }
            } else {
                for (S3ObjectSummary s3ObjectSummary : s3ObjectSummaries) {
                    int len = s3ObjectSummary.getKey().split(Constants.S3_KEY_SEPARATOR).length;
                    if (len != 3) {
                        logger.error("No instance defined on configuration but the backup contains one or more instances. Please check.");
                        stop();
                        return Collections.emptyList();
                    }
                }
            }
            s3ObjectSummaries.stream().forEach(s3ObjectSummary -> {
                String[] keys = s3ObjectSummary.getKey().split(Constants.S3_KEY_SEPARATOR);
                int partition = Integer.parseInt(keys[positionToSplit]);
                if (!mapTopicPartitionRecord.containsKey(getKey(restoreTopicName))) {
                    mapTopicPartitionRecord.put(getKey(restoreTopicName), new PartitionRecord(restoreTopicName.getS3TopicName(), restoreTopicName.getKafkaTopicName()));
                }
                mapTopicPartitionRecord.get(getKey(restoreTopicName)).addPartition(partition);
            });
        }
        Iterator<String> it = mapTopicPartitionRecord.keySet().iterator();
        while (it.hasNext()) {
            countTotalPartitions += mapTopicPartitionRecord.get(it.next()).getPartitions().size();
        }

        List<PartitionRecord> partitionRecordList = mapTopicPartitionRecord.values().stream().collect(Collectors.toList());
        Collections.sort(partitionRecordList, Comparator.comparing(PartitionRecord::getSizePartitions).reversed());

        List<Map<String, String>> taskConfigs = new ArrayList<>(maxTasks);

        if (maxTasks >= countTotalPartitions) {
            if (maxTasks > countTotalPartitions) {
                logger.error("Number of task {} greather than the partitions present inside the backup {}. ", maxTasks, countTotalPartitions);
                stop();
                return Collections.emptyList();
            }

            maxTasks = countTotalPartitions;
            int countPosTopic = 0;
            int lastIndex = 0;
            for (int i = 0; i < maxTasks; ++i) {
                Map<String, String> taskProps = new HashMap<>(connectorConfig.originalsStrings());
                PartitionRecord record = null;
                while (record == null) {
                    PartitionRecord tmp = partitionRecordList.get(countPosTopic);
                    if (tmp.getSizePartitions() > 0 && lastIndex < tmp.getSizePartitions()) {
                        record = tmp;
                    } else {
                        countPosTopic++;
                        lastIndex = 0;
                    }
                }

                Integer[] partitions = record.getPartitions().toArray(new Integer[record.getSizePartitions()]);
                taskProps.put(Constants.KEY_S3_TOPIC_NAME_TASK, record.getS3TopicName());
                taskProps.put(Constants.KEY_TOPIC_NAME_TASK, record.getKafkaTopicName());
                taskProps.put(Constants.KEY_PARTITION_TASK, String.valueOf(partitions[lastIndex]));
                lastIndex++;
                taskConfigs.add(taskProps);

            }
        } else if (maxTasks < countTotalPartitions) {
            int countPosTopic = 0;
            int lastIndex = 0;
            int indexTask = 1;

            if (maxTasks < partitionRecordList.size()) {
                logger.error("Number of task {} less than the partitions present inside the backup {}. ", maxTasks, countTotalPartitions);
                stop();
                return Collections.emptyList();
            }

            int numTaskForTopic = maxTasks / partitionRecordList.size();
            int diffTasks = maxTasks % partitionRecordList.size();
            for (PartitionRecord partitionRecord : partitionRecordList) {
                if (partitionRecord.getSizePartitions() >= numTaskForTopic) {
                    partitionRecord.setNumTask(numTaskForTopic);
                } else {
                    partitionRecord.setNumTask(partitionRecord.getSizePartitions());
                }
            }

            while (diffTasks > 0) {
                for (PartitionRecord partitionRecord : partitionRecordList) {
                    if (diffTasks > 0 && partitionRecord.getSizePartitions() >= (partitionRecord.getNumTask() + 1)) {
                        partitionRecord.setNumTask(partitionRecord.getNumTask() + 1);
                        diffTasks--;
                    }
                }
            }

            for (int i = 0; i < maxTasks; ++i) {
                Map<String, String> taskProps = new HashMap<>(connectorConfig.originalsStrings());
                PartitionRecord record = null;
                while (record == null) {
                    PartitionRecord tmp = partitionRecordList.get(countPosTopic);
                    if (tmp.getSizePartitions() > 0 && lastIndex < tmp.getSizePartitions()) {
                        record = tmp;
                    } else {
                        countPosTopic++;
                        lastIndex = 0;
                        indexTask = 1;
                    }
                }

                Integer[] partitions = record.getPartitions().toArray(new Integer[record.getSizePartitions()]);

                int block = record.getSizePartitions() / record.getNumTask();
                int lastEndReadIndex = lastIndex + block;
                if (indexTask == record.getNumTask()) {
                    lastEndReadIndex = partitions.length;
                }
                taskProps.put(Constants.KEY_S3_TOPIC_NAME_TASK, record.getS3TopicName());
                taskProps.put(Constants.KEY_TOPIC_NAME_TASK, record.getKafkaTopicName());
                String partitionsTask = "";
                for (int j = lastIndex; j < lastEndReadIndex; j++) {
                    partitionsTask += partitions[j];
                    if ((j + 1) < lastEndReadIndex) {
                        partitionsTask += ";";
                    }
                }
                taskProps.put(Constants.KEY_PARTITION_TASK, partitionsTask);
                lastIndex = lastEndReadIndex;
                indexTask++;
                taskConfigs.add(taskProps);
            }
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

    private String getKey(RestoreTopicName restoreTopicName) {
        return restoreTopicName.getS3TopicName() + "-" + restoreTopicName.getKafkaTopicName();
    }
}
