package ch.tbd.kafka.backuprestore.backup.storage.partitioner;

import org.apache.kafka.connect.sink.SinkRecord;

/**
 * Class Partitioner.
 * This represents TODO.
 *
 * @author iorfinoa
 * @version $$Revision$$
 */
public interface Partitioner<T> {

    String encodePartition(SinkRecord sinkRecord);
}
