package ch.tbd.kafka.backuprestore.restore.deserializers.avro;

import ch.tbd.kafka.backuprestore.model.KafkaRecord;
import ch.tbd.kafka.backuprestore.model.avro.AvroKafkaRecord;
import ch.tbd.kafka.backuprestore.restore.deserializers.KafkaRecordDeserializer;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class KafkaRecordAvroDeserializer implements KafkaRecordDeserializer {

    @Override
    public KafkaRecord deserialize(ByteBuffer record) {
        try {
            AvroKafkaRecord avroKafkaRecord = AvroKafkaRecord.fromByteBuffer(record);
            return new KafkaRecord(avroKafkaRecord.getTopic().toString(), avroKafkaRecord.getPartition(), avroKafkaRecord.getOffset(), avroKafkaRecord.getTimestamp(), avroKafkaRecord.getKey(), avroKafkaRecord.getValue(), adaptHeaders(avroKafkaRecord.getHeaders()));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Map<String, ByteBuffer> adaptHeaders(Map<CharSequence, ByteBuffer> headers) {
        if (headers == null) {
            return null;
        }
        Map<String, ByteBuffer> newHeaders = new HashMap<>();
        headers.forEach((key, value) -> {
            newHeaders.put(key.toString(), value);
        });
        return newHeaders;
    }

}
