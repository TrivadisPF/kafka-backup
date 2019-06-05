package ch.tbd.kafka.backuprestore.backup.serializers.avro;

import ch.tbd.kafka.backuprestore.backup.serializers.KafkaRecordSerializer;
import ch.tbd.kafka.backuprestore.model.KafkaRecord;
import ch.tbd.kafka.backuprestore.model.avro.AvroKafkaRecord;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class KafkaRecordAvroSerializer implements KafkaRecordSerializer {

    @Override
    public ByteBuffer serialize(KafkaRecord record) {
        AvroKafkaRecord avroKafkaRecord = AvroKafkaRecord.newBuilder()
                .setTopic(record.getTopic())
                .setPartition(record.getPartition())
                .setOffset(record.getOffset())
                .setTimestamp(record.getTimestamp())
                .setKey(record.getKey())
                .setValue(record.getValue())
                .setHeaders(adaptHeaders(record.getHeaders()))
                .build();
        try {
            return avroKafkaRecord.toByteBuffer();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Map<CharSequence, ByteBuffer> adaptHeaders(Map<String, ByteBuffer> headers) {
        if (headers == null) {
            return null;
        }
        Map<CharSequence, ByteBuffer> newHeaders = new HashMap<>();
        headers.forEach((key, value) -> {
            newHeaders.put(key, value);
        });
        return newHeaders;
    }
}
