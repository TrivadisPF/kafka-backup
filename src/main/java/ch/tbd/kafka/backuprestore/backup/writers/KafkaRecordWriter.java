package ch.tbd.kafka.backuprestore.backup.writers;

import ch.tbd.kafka.backuprestore.model.KafkaRecord;

import java.util.List;

public interface KafkaRecordWriter {

    void write(List<KafkaRecord> records);

}
