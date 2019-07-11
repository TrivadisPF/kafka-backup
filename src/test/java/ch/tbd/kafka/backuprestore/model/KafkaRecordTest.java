/*
 * ------------------------------------------------------------------------------------------------
 * Copyright 2014 by Swiss Post, Information Technology Services
 * ------------------------------------------------------------------------------------------------
 * $Id$
 * ------------------------------------------------------------------------------------------------
 */

package ch.tbd.kafka.backuprestore.model;

import org.apache.kafka.connect.data.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Class KafkaRecordTest.
 * This represents TODO.
 *
 * @author iorfinoa
 * @version $$Revision$$
 */
public class KafkaRecordTest {
    private String TOPIC_NAME = "TOPIC_NAME";
    private int PARTITION = 0;
    private long OFFSET = 10L;
    private long TIMESTAMP = 100L;
    private String KEY_STRING = "KEY";
    private ByteBuffer KEY = ByteBuffer.wrap(KEY_STRING.getBytes());
    private String VALUE_STRING = "VALUE";
    private ByteBuffer VALUE = ByteBuffer.wrap(VALUE_STRING.getBytes());

    private KafkaRecord kafkaRecord;

    @BeforeEach
    public void init() {
        Map<String, ByteBuffer> headers = new HashMap<>();
        headers.put("HEADER1", ByteBuffer.wrap("HEADER1".getBytes()));
        headers.put("HEADER2", ByteBuffer.wrap("HEADER2".getBytes()));
        this.kafkaRecord = new KafkaRecord(TOPIC_NAME, PARTITION, OFFSET, TIMESTAMP, KEY, VALUE, headers);
    }

    @Test
    public void testInit() {
        Assertions.assertEquals(TOPIC_NAME, this.kafkaRecord.getTopic());
        Assertions.assertEquals(PARTITION, this.kafkaRecord.getPartition());
        Assertions.assertEquals(OFFSET, this.kafkaRecord.getOffset());
        Assertions.assertEquals(TIMESTAMP, this.kafkaRecord.getTimestamp());
        Assertions.assertNotNull(this.kafkaRecord.getKey());
        Assertions.assertEquals(KEY, this.kafkaRecord.getKey());
        Assertions.assertEquals(KEY_STRING, new String(this.kafkaRecord.getKey().array()));
        Assertions.assertNotNull(this.kafkaRecord.getValue());
        Assertions.assertEquals(VALUE, this.kafkaRecord.getValue());
        Assertions.assertEquals(VALUE_STRING, new String(this.kafkaRecord.getValue().array()));
        Assertions.assertNotNull(this.kafkaRecord.getHeaders());
        Assertions.assertTrue(this.kafkaRecord.hasHeaders());
        Assertions.assertFalse(this.kafkaRecord.getHeaders().isEmpty());
        Assertions.assertEquals(2, this.kafkaRecord.getHeaders().keySet().size());
    }

    @Test
    public void testConstructor1() {
        TOPIC_NAME = TOPIC_NAME + new Random().nextInt();
        PARTITION = new Random().nextInt();
        OFFSET = new Random().nextLong();
        TIMESTAMP = new Random().nextLong();
        KEY_STRING = KEY_STRING + new Random().nextInt();
        KEY = ByteBuffer.wrap(new String(KEY_STRING).getBytes());
        VALUE_STRING = VALUE_STRING + new Random().nextInt();
        VALUE = ByteBuffer.wrap(new String(VALUE_STRING).getBytes());
        this.kafkaRecord = new KafkaRecord(TOPIC_NAME, PARTITION, OFFSET, TIMESTAMP, KEY, VALUE);

        Assertions.assertEquals(TOPIC_NAME, this.kafkaRecord.getTopic());
        Assertions.assertEquals(PARTITION, this.kafkaRecord.getPartition());
        Assertions.assertEquals(OFFSET, this.kafkaRecord.getOffset());
        Assertions.assertEquals(TIMESTAMP, this.kafkaRecord.getTimestamp());
        Assertions.assertTrue(this.kafkaRecord.hasKey());
        Assertions.assertNotNull(this.kafkaRecord.getKey());
        Assertions.assertEquals(KEY, this.kafkaRecord.getKey());
        Assertions.assertEquals(KEY_STRING, new String(this.kafkaRecord.getKey().array()));
        Assertions.assertNotNull(this.kafkaRecord.getValue());
        Assertions.assertEquals(VALUE, this.kafkaRecord.getValue());
        Assertions.assertEquals(VALUE_STRING, new String(this.kafkaRecord.getValue().array()));
        Assertions.assertFalse(this.kafkaRecord.hasHeaders());

        Assertions.assertNull(this.kafkaRecord.getKeySchema());

        this.kafkaRecord.addHeader("HEADER3", "HEADER3".getBytes());
        Assertions.assertTrue(this.kafkaRecord.hasHeaders());
        Assertions.assertEquals(1, this.kafkaRecord.getHeaders().keySet().size());
    }

    @Test
    public void addHeaders() {
        this.kafkaRecord.addHeader("HEADER3", "HEADER3".getBytes());
        Assertions.assertTrue(this.kafkaRecord.hasHeaders());
        Assertions.assertEquals(3, this.kafkaRecord.getHeaders().keySet().size());
    }

    @Test
    public void testToString() {
        String str = "KafkaRecord{topic=" + TOPIC_NAME + ", partition=" + PARTITION + ", offset=" + OFFSET +
                ", timestamp=" + TIMESTAMP +
                ", key=" + KEY +
                '}';

        Assertions.assertEquals(str, this.kafkaRecord.toString());
    }


}
