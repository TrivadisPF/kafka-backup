package ch.tbd.kafka.backuprestore.restore.offsets;

public interface OffsetsIndexRepository {
    void store(String oldTopic, int oldPartition, long oldOffset, long oldTimestamp, String newTopic, int newPartition, long newOffset, long newTimestamp);
}
