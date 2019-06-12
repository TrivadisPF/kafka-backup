package ch.tbd.kafka.backuprestore.backup;

import ch.tbd.kafka.backuprestore.backup.serializers.KafkaRecordSerializer;
import ch.tbd.kafka.backuprestore.backup.serializers.avro.KafkaRecordAvroSerializer;
import ch.tbd.kafka.backuprestore.backup.writers.KafkaRecordWriter;
import ch.tbd.kafka.backuprestore.backup.writers.s3.S3KafkaRecordWriter;
import ch.tbd.kafka.backuprestore.model.KafkaRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteBufferDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class Backup {
    private static final Logger LOGGER = LoggerFactory.getLogger(Backup.class);

    private String region;
    private String bucket;
    private String bootstrapServer;

    public Backup(String region, String bucket, String bootstrapServer) {
        this.region = region;
        this.bucket = bucket;
        this.bootstrapServer = bootstrapServer;
    }

    public void backup(String topic) {
        final Properties config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "Backup");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteBufferDeserializer.class.getName());
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteBufferDeserializer.class.getName());
        //TODO: configurable?
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        KafkaRecordSerializer kafkaRecordSerializer = new KafkaRecordAvroSerializer();
        KafkaRecordWriter kafkaRecordWriter = new S3KafkaRecordWriter(region, bucket, null, 0, kafkaRecordSerializer);

        try (final Consumer<ByteBuffer, ByteBuffer> consumer = new KafkaConsumer<>(config)) {
            consumer.subscribe(Collections.singletonList(topic));

            while (true) {
                final ConsumerRecords<ByteBuffer, ByteBuffer> consumerRecords = consumer.poll(Duration.ofMillis(1000));
                List<KafkaRecord> kafkaRecords = new ArrayList<>();
                consumerRecords.forEach(record -> {
                    KafkaRecord kafkaRecord = new KafkaRecord(record.topic(), record.partition(), record.offset(), record.timestamp(), record.key(), record.value());
                    if (record.headers() != null) {
                        record.headers().forEach(header ->
                                kafkaRecord.addHeader(header.key(), header.value())
                        );
                    }
                    kafkaRecords.add(kafkaRecord);
                });
                if (!kafkaRecords.isEmpty()) {
                    kafkaRecordWriter.write(kafkaRecords);
                }
                //TODO Exactly once??!!!
                consumer.commitAsync((offsets, exception) -> {
                    if (exception == null) {
                        offsets.forEach((topicPartition, offsetAndMetadata) -> {
                            LOGGER.debug("{}@{}", topicPartition.toString(), offsetAndMetadata.offset());
                        });
                    } else {
                        // TODO: this should be handled
                        LOGGER.error("Error committing records", exception);
                    }
                });
            }
        }
    }
}