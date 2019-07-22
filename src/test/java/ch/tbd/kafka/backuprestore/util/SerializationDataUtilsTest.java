package ch.tbd.kafka.backuprestore.util;

import ch.tbd.kafka.backuprestore.model.avro.AvroKafkaRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Date;

/**
 * Class SerializationDataUtilsTest.
 * This represents TODO.
 *
 * @author iorfinoa
 * @version $$Revision$$
 */
public class SerializationDataUtilsTest {

    private byte[] avroDataSerialized = {-84, -19, 0, 5, 115, 114, 0, 53, 99, 104, 46, 116, 98, 100, 46, 107, 97, 102,
            107, 97, 46, 98, 97, 99, 107, 117, 112, 114, 101, 115, 116, 111, 114, 101, 46, 109, 111, 100, 101, 108, 46,
            97, 118, 114, 111, 46, 65, 118, 114, 111, 75, 97, 102, 107, 97, 82, 101, 99, 111, 114, 100, 116, -54, 94,
            -38, 74, -46, -113, -84, 12, 0, 0, 120, 114, 0, 43, 111, 114, 103, 46, 97, 112, 97, 99, 104, 101, 46, 97,
            118, 114, 111, 46, 115, 112, 101, 99, 105, 102, 105, 99, 46, 83, 112, 101, 99, 105, 102, 105, 99, 82, 101,
            99, 111, 114, 100, 66, 97, 115, 101, 2, -94, -7, -84, -58, -73, 52, 29, 12, 0, 0, 120, 112, 119, 33, 20,
            116, 111, 112, 105, 99, 45, 116, 101, 115, 116, 2, 0, -26, -70, -46, -54, -128, 91, 0, 6, 75, 69, 89, 0,
            10, 86, 65, 76, 85, 69, 0, 0, 120};


    private AvroKafkaRecord avroKafkaRecord = AvroKafkaRecord.newBuilder()
            .setKey(ByteBuffer.wrap("KEY".getBytes()))
            .setValue(ByteBuffer.wrap("VALUE".getBytes()))
            .setTimestamp(new Date().getTime())
            .setOffset(0L).setPartition(1).setTopic("topic-test").build();

    @Test
    public void testSerializeNull() {
        Assertions.assertNull(SerializationDataUtils.serialize(null));
    }

    @Test
    public void testSerializeException() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            SerializationDataUtils.serialize(new Object());
        });
    }

    @Test
    public void testSerialize() {
        byte[] bytes = SerializationDataUtils.serialize(avroKafkaRecord);
        Assertions.assertEquals(avroDataSerialized.length, bytes.length);
    }

    @Test
    public void testDeserializeNull() {
        Assertions.assertNull(SerializationDataUtils.deserialize(null));
    }

    @Test
    public void testDeserializeException() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            SerializationDataUtils.deserialize("".getBytes());
        });
    }

    @Test
    public void testDeserialize() {
        AvroKafkaRecord record = (AvroKafkaRecord) SerializationDataUtils.deserialize(avroDataSerialized);
        if (record == null) {
            Assertions.fail();
        }
        Assertions.assertEquals(avroKafkaRecord.getKey(), record.getKey());
        Assertions.assertEquals(avroKafkaRecord.getValue(), record.getValue());
        Assertions.assertEquals(avroKafkaRecord.getHeaders(), record.getHeaders());
        Assertions.assertEquals(avroKafkaRecord.getOffset(), record.getOffset());
        Assertions.assertEquals(avroKafkaRecord.getTopic().toString(), record.getTopic().toString());
    }

}
