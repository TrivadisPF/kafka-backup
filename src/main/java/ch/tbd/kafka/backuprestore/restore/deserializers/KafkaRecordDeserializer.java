package ch.tbd.kafka.backuprestore.restore.deserializers;

import ch.tbd.kafka.backuprestore.model.KafkaRecord;

import java.nio.ByteBuffer;

public interface KafkaRecordDeserializer {

    KafkaRecord deserialize(ByteBuffer record);

}
