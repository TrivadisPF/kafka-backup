package ch.tbd.kafka.backuprestore.backup.storage.format;

import org.apache.kafka.connect.sink.SinkRecord;

import java.io.Closeable;

/**
 * Class RecordWriter.
 * This represents TODO.
 *
 * @author iorfinoa
 * @version $$Revision$$
 */
public interface RecordWriter extends Closeable {

    void write(SinkRecord record);

    void close();

    void commit();
}
