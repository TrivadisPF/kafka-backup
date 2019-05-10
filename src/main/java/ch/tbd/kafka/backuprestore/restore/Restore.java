package ch.tbd.kafka.backuprestore.restore;

import ch.tbd.kafka.backuprestore.model.KafkaRecord;
import ch.tbd.kafka.backuprestore.restore.deserializers.KafkaRecordDeserializer;
import ch.tbd.kafka.backuprestore.restore.deserializers.avro.KafkaRecordAvroDeserializer;
import ch.tbd.kafka.backuprestore.restore.offsets.LoggingOffsetsIndexRepository;
import ch.tbd.kafka.backuprestore.restore.offsets.OffsetsIndexRepository;
import ch.tbd.kafka.backuprestore.restore.readers.KafkaRecordReader;
import ch.tbd.kafka.backuprestore.restore.readers.s3.S3KafkaRecordReader;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteBufferSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.time.LocalDateTime;
import java.time.temporal.ChronoField;
import java.util.List;
import java.util.Properties;

//TODO: parametrize: strategy for headers, timestamps, and topicName, restore to a different kafka cluster
public class Restore {

    private static final Logger LOGGER = LoggerFactory.getLogger(Restore.class);

    private static final String RESTORED_HEADER = "x-restored";

    private final Properties config = new Properties();
    private String region;
    private String bucket;
    private String bootstrapServer;

    public Restore(String region, String bucket, String bootstrapServer) {
        this.region = region;
        this.bucket = bucket;
        this.bootstrapServer = bootstrapServer;
    }

    public void restore(String topic) {
        restore(topic, topic);
    }

    public void restore(String topicSource, String topicDestination) {
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteBufferSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteBufferSerializer.class.getName());

        KafkaRecordDeserializer kafkaRecordDeserializer = new KafkaRecordAvroDeserializer();
        KafkaRecordReader kafkaRecordReader = new S3KafkaRecordReader(region, bucket, kafkaRecordDeserializer);
        OffsetsIndexRepository offsetsIndexRepository = new LoggingOffsetsIndexRepository();

        Producer<ByteBuffer, ByteBuffer> producer = new KafkaProducer<>(config);

        List<KafkaRecord> kafkaRecords = kafkaRecordReader.read(topicSource);

        kafkaRecords.forEach(kafkaRecord -> {
            final ProducerRecord<ByteBuffer, ByteBuffer> record = new ProducerRecord(
                    topicDestination,
                    kafkaRecord.getPartition(), kafkaRecord.getTimestamp(),
                    kafkaRecord.getKey(), kafkaRecord.getValue()
            );

            if (kafkaRecord.hasHeaders()) {
                kafkaRecord.getHeaders().forEach((key, value) -> record.headers().add(new RecordHeader(key, value)));
            }
            record.headers().add(new RecordHeader(RESTORED_HEADER, Long.toString(LocalDateTime.now().getLong(ChronoField.MICRO_OF_SECOND)).getBytes(Charset.forName("UTF-8"))));

            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    throw new RuntimeException(exception);
                } else {
                    //TODO: may check that the record has been restored on the correct topic and partition....
                    offsetsIndexRepository.store(kafkaRecord.getTopic(), kafkaRecord.getPartition(), kafkaRecord.getOffset(), kafkaRecord.getTimestamp(), metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp());
                }
            });
        });
        producer.close();
    }

}
