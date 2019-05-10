package ch.tbd.kafka.backuprestore.backup.serializers;

import ch.tbd.kafka.backuprestore.model.KafkaRecord;

import java.nio.ByteBuffer;

public interface KafkaRecordSerializer {

    ByteBuffer serialize(KafkaRecord record);

}
