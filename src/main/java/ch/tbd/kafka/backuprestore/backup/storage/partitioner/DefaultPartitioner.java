package ch.tbd.kafka.backuprestore.backup.storage.partitioner;

import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Map;

/**
 * Class DefaultPartitioner.
 * This represents TODO.
 *
 * @author iorfinoa
 * @version $$Revision$$
 */
public class DefaultPartitioner<T> implements Partitioner<T> {
    private static final String TOPIC_FIELD = "topic";
    private static final String PARTITION_FIELD = "partition";

    private static final String SCHEMA_GENERATOR_CLASS =
            "ch.tbd.kafka.backuprestore.backup.storage.partitioner.DefaultSchemaGenerator";

    protected Map<String, Object> config;

    @Override
    public String encodePartition(SinkRecord sinkRecord) {
        return TOPIC_FIELD + "=" + sinkRecord.topic() + ";" + PARTITION_FIELD + "=" + sinkRecord.kafkaPartition();
    }

}
