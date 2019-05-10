package ch.tbd.kafka.backuprestore.restore.readers;

import ch.tbd.kafka.backuprestore.model.KafkaRecord;

import java.util.List;

//TODO return a stream of records
//TODO define what can be restored
public interface KafkaRecordReader {

    List<KafkaRecord> read(String topic);

    List<KafkaRecord> read(String topic, int partition);

    KafkaRecord read(String topic, int partition, long offset);

    List<KafkaRecord> readBetweenTimestamps(String topic, int partition, long from, long to);

    List<KafkaRecord> readBetweenOffsets(String topic, int partition, long from, long to);
}
