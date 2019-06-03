package ch.tbd.kafka.backuprestore.model;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class KafkaRecordNew implements Serializable {

    private String topic;
    private int partition;
    private long offset;
    private long timestamp;
    private ByteBuffer key;
    private ByteBuffer value;
    private Map<String, ByteBuffer> headers;

    public KafkaRecordNew(String topic, int partition, long offset, long timestamp, ByteBuffer key, ByteBuffer value) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.timestamp = timestamp;
        this.key = key;
        this.value = value;
    }

    public KafkaRecordNew(String topic, int partition, long offset, long timestamp, ByteBuffer key, ByteBuffer value, Map<String, ByteBuffer> headers) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.timestamp = timestamp;
        this.key = key;
        this.value = value;
        this.headers = headers;
    }

    public String getTopic() {
        return topic;
    }

    public int getPartition() {
        return partition;
    }

    public long getOffset() {
        return offset;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public ByteBuffer getKey() {
        return key;
    }

    public ByteBuffer getValue() {
        return value;
    }

    public Map<String, ByteBuffer> getHeaders() {
        return headers;
    }

    public void addHeader(String key, byte[] value) {
        if (headers == null) {
            headers = new HashMap<>();
        }
        headers.put(key, ByteBuffer.wrap(value));
    }

    public boolean hasKey() {
        return !(key == null);
    }

    public boolean hasHeaders() {
        if (headers == null || headers.size() == 0) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "KafkaRecord{" +
                "topic=" + topic +
                ", partition=" + partition +
                ", offset=" + offset +
                ", timestamp=" + timestamp +
                ", key=" + key +
                '}';
    }
}
