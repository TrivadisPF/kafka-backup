package ch.tbd.kafka.backuprestore.restore.offsets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingOffsetsIndexRepository implements OffsetsIndexRepository {

    private static final Logger LOGGER = LoggerFactory.getLogger(LoggingOffsetsIndexRepository.class);

    @Override
    public void store(String oldTopic, int oldPartition, long oldOffset, long oldTimestamp, String newTopic, int newPartition, long newOffset, long newTimestamp) {
        LOGGER.info(format(oldTopic, oldPartition, oldOffset, oldTimestamp) + " -> " + format(newTopic, newPartition, newOffset, newTimestamp));
    }

    private String format(String topic, int partition, long offset, long timestamp) {
        return String.format("%s@%d:%d(%d)", topic, partition, offset, timestamp);
    }
}
