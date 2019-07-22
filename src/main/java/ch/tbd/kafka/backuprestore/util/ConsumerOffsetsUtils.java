package ch.tbd.kafka.backuprestore.util;

import ch.tbd.kafka.backuprestore.restore.consumer_group.KeyConsumerGroup;
import ch.tbd.kafka.backuprestore.restore.consumer_group.ValueConsumerGroup;
import org.apache.kafka.common.protocol.types.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * Class ConsumerOffsetsUtils.
 * This represents TODO.
 *
 * @author iorfinoa
 * @version $$Revision$$
 */
public class ConsumerOffsetsUtils {

    private static Logger logger = LoggerFactory.getLogger(ConsumerOffsetsUtils.class);

    private static Schema OFFSET_COMMIT_KEY_SCHEMA = new Schema(new Field("group", Type.STRING),
            new Field("topic", Type.STRING),
            new Field("partition", Type.INT32));
    private static BoundField OFFSET_KEY_GROUP_FIELD = OFFSET_COMMIT_KEY_SCHEMA.get("group");
    private static BoundField OFFSET_KEY_TOPIC_FIELD = OFFSET_COMMIT_KEY_SCHEMA.get("topic");
    private static BoundField OFFSET_KEY_PARTITION_FIELD = OFFSET_COMMIT_KEY_SCHEMA.get("partition");

    private static Schema GROUP_METADATA_KEY_SCHEMA = new Schema(new Field("group", Type.STRING));
    private static BoundField GROUP_KEY_GROUP_FIELD = GROUP_METADATA_KEY_SCHEMA.get("group");

    private static Map<Integer, Schema> OFFSET_KEY_SCHEMAS = new HashMap<Integer, Schema>() {
        {
            put(0, OFFSET_COMMIT_KEY_SCHEMA);
            put(1, OFFSET_COMMIT_KEY_SCHEMA);
            put(2, GROUP_METADATA_KEY_SCHEMA);
        }
    };

    private static Schema OFFSET_COMMIT_VALUE_SCHEMA_V0 = new Schema(new Field("offset", Type.INT64),
            new Field("metadata", Type.STRING, "Associated metadata.", ""),
            new Field("timestamp", Type.INT64));
    private static BoundField OFFSET_VALUE_OFFSET_FIELD_V0 = OFFSET_COMMIT_VALUE_SCHEMA_V0.get("offset");
    private static BoundField OFFSET_VALUE_METADATA_FIELD_V0 = OFFSET_COMMIT_VALUE_SCHEMA_V0.get("metadata");
    private static BoundField OFFSET_VALUE_TIMESTAMP_FIELD_V0 = OFFSET_COMMIT_VALUE_SCHEMA_V0.get("timestamp");


    private static Schema OFFSET_COMMIT_VALUE_SCHEMA_V1 = new Schema(new Field("offset", Type.INT64),
            new Field("metadata", Type.STRING, "Associated metadata.", ""),
            new Field("commit_timestamp", Type.INT64),
            new Field("expire_timestamp", Type.INT64));
    private static BoundField OFFSET_VALUE_OFFSET_FIELD_V1 = OFFSET_COMMIT_VALUE_SCHEMA_V1.get("offset");
    private static BoundField OFFSET_VALUE_METADATA_FIELD_V1 = OFFSET_COMMIT_VALUE_SCHEMA_V1.get("metadata");
    private static BoundField OFFSET_VALUE_COMMIT_TIMESTAMP_FIELD_V1 = OFFSET_COMMIT_VALUE_SCHEMA_V1.get("commit_timestamp");
    private static BoundField OFFSET_VALUE_EXPIRE_TIMESTAMP_FIELD_V1 = OFFSET_COMMIT_VALUE_SCHEMA_V1.get("expire_timestamp");

    private static Schema OFFSET_COMMIT_VALUE_SCHEMA_V2 = new Schema(new Field("offset", Type.INT64),
            new Field("metadata", Type.STRING, "Associated metadata.", ""),
            new Field("commit_timestamp", Type.INT64));
    private static BoundField OFFSET_VALUE_OFFSET_FIELD_V2 = OFFSET_COMMIT_VALUE_SCHEMA_V2.get("offset");
    private static BoundField OFFSET_VALUE_METADATA_FIELD_V2 = OFFSET_COMMIT_VALUE_SCHEMA_V2.get("metadata");
    private static BoundField OFFSET_VALUE_COMMIT_TIMESTAMP_FIELD_V2 = OFFSET_COMMIT_VALUE_SCHEMA_V2.get("commit_timestamp");

    private static Schema OFFSET_COMMIT_VALUE_SCHEMA_V3 = new Schema(
            new Field("offset", Type.INT64),
            new Field("leader_epoch", Type.INT32),
            new Field("metadata", Type.STRING, "Associated metadata.", ""),
            new Field("commit_timestamp", Type.INT64));
    private static BoundField OFFSET_VALUE_OFFSET_FIELD_V3 = OFFSET_COMMIT_VALUE_SCHEMA_V3.get("offset");
    private static BoundField OFFSET_VALUE_LEADER_EPOCH_FIELD_V3 = OFFSET_COMMIT_VALUE_SCHEMA_V3.get("leader_epoch");
    private static BoundField OFFSET_VALUE_METADATA_FIELD_V3 = OFFSET_COMMIT_VALUE_SCHEMA_V3.get("metadata");
    private static BoundField OFFSET_VALUE_COMMIT_TIMESTAMP_FIELD_V3 = OFFSET_COMMIT_VALUE_SCHEMA_V3.get("commit_timestamp");

    private static Map<Integer, Schema> OFFSET_VALUE_SCHEMAS = new HashMap<Integer, Schema>() {{
        put(0, OFFSET_COMMIT_VALUE_SCHEMA_V0);
        put(1, OFFSET_COMMIT_VALUE_SCHEMA_V1);
        put(2, OFFSET_COMMIT_VALUE_SCHEMA_V2);
        put(3, OFFSET_COMMIT_VALUE_SCHEMA_V3);
    }};


    public static KeyConsumerGroup readMessageKey(ByteBuffer buffer) {
        KeyConsumerGroup keyConsumerGroup = null;
        try {
            short version = buffer.getShort();
            Schema keySchema = schemaKeyFor(version);
            Struct key = keySchema.read(buffer);
            if (version == 0 || version == 1) {
                String group = key.getString(OFFSET_KEY_GROUP_FIELD);
                String topic = key.getString(OFFSET_KEY_TOPIC_FIELD);
                int partition = key.getInt(OFFSET_KEY_PARTITION_FIELD);
                keyConsumerGroup = new KeyConsumerGroup(version, group, topic, partition);
                logger.info("Version key {} - Consumer group {} - Topic {} - Partition {}", version, group, topic, partition);
            } else if (version == 2) {
                String group = key.getString(GROUP_KEY_GROUP_FIELD);
                keyConsumerGroup = new KeyConsumerGroup(version, group);
                logger.info("Version key {} - Consumer group {}", version, group);
            } else {
                throw new IllegalStateException("Unknown offset key version: " + version);
            }

        } catch (SchemaException e) {
            logger.warn(e.getMessage());
        }
        return keyConsumerGroup;
    }

    public static ValueConsumerGroup readMessageValue(ByteBuffer buffer) {
        ValueConsumerGroup valueConsumerGroup = null;
        if (buffer == null) {
            logger.warn("No value ");
        } else {
            short version = buffer.getShort();
            Schema valueSchema = schemaValueFor(version);
            try {
                Struct value = valueSchema.read(buffer);
                if (version == 0) {
                    long offset = value.getLong(OFFSET_VALUE_OFFSET_FIELD_V0);
                    String metadata = value.getString(OFFSET_VALUE_METADATA_FIELD_V0);
                    long timestamp = value.getLong(OFFSET_VALUE_TIMESTAMP_FIELD_V0);
                    valueConsumerGroup = new ValueConsumerGroup(version, offset, metadata, timestamp);
                    logger.info("Version value {} - Offset {} - metadata {} - timestamp {}", version, offset, metadata, timestamp);
                } else if (version == 1) {
                    long offset = value.getLong(OFFSET_VALUE_OFFSET_FIELD_V1);
                    String metadata = value.getString(OFFSET_VALUE_METADATA_FIELD_V1);
                    long commitTimestamp = value.getLong(OFFSET_VALUE_COMMIT_TIMESTAMP_FIELD_V1);
                    long expireTimestamp = value.getLong(OFFSET_VALUE_EXPIRE_TIMESTAMP_FIELD_V1);
                    valueConsumerGroup = new ValueConsumerGroup(version, offset, metadata, commitTimestamp, expireTimestamp);
                    logger.info("Version value {} - Offset {} - metadata {} - commitTimestamp {} - expireTimestamp {}", version, offset, metadata, commitTimestamp, expireTimestamp);
                } else if (version == 2) {
                    long offset = value.getLong(OFFSET_VALUE_OFFSET_FIELD_V2);
                    String metadata = value.getString(OFFSET_VALUE_METADATA_FIELD_V2);
                    long timestamp = value.getLong(OFFSET_VALUE_COMMIT_TIMESTAMP_FIELD_V2);
                    valueConsumerGroup = new ValueConsumerGroup(version, offset, metadata, timestamp);
                    logger.info("Version value {} - Offset {} - metadata {} - timestamp {}", version, offset, metadata, timestamp);
                } else if (version == 3) {
                    long offset = value.getLong(OFFSET_VALUE_OFFSET_FIELD_V3);
                    int leaderEpoch = value.getInt(OFFSET_VALUE_LEADER_EPOCH_FIELD_V3);
                    String metadata = value.getString(OFFSET_VALUE_METADATA_FIELD_V3);
                    long timestamp = value.getLong(OFFSET_VALUE_COMMIT_TIMESTAMP_FIELD_V3);
                    valueConsumerGroup = new ValueConsumerGroup(version, offset, leaderEpoch, metadata, timestamp);
                    logger.info("Version value {} - Offset {} - leaderEpoch {} - metadata {} - timestamp {}", version, offset, leaderEpoch, metadata, timestamp);
                } else {
                    throw new IllegalStateException("Unknown offset message version: " + version);
                }
            } catch (Exception e) {
                logger.warn(e.getMessage());
            }
        }
        return valueConsumerGroup;
    }

    private static Schema schemaKeyFor(int version) {
        return OFFSET_KEY_SCHEMAS.get(version);
    }

    private static Schema schemaValueFor(int version) {
        return OFFSET_VALUE_SCHEMAS.get(version);
    }
}
