package ch.tbd.kafka.backuprestore.backup.writers;

import ch.tbd.kafka.backuprestore.backup.serializers.KafkaRecordSerializer;

public abstract class AbstractKafkaRecordWriter implements KafkaRecordWriter {

    protected KafkaRecordSerializer kafkaRecordSerializer;

    public AbstractKafkaRecordWriter(KafkaRecordSerializer kafkaRecordSerializer) {
        this.kafkaRecordSerializer = kafkaRecordSerializer;
    }

}
