package ch.tbd.kafka.backuprestore.restore.readers;

import ch.tbd.kafka.backuprestore.restore.deserializers.KafkaRecordDeserializer;

public abstract class AbstractKafkaRecordReader implements KafkaRecordReader {

    protected KafkaRecordDeserializer kafkaRecordDeserializer;

    public AbstractKafkaRecordReader(KafkaRecordDeserializer kafkaRecordDeserializer) {
        this.kafkaRecordDeserializer = kafkaRecordDeserializer;
    }

}
