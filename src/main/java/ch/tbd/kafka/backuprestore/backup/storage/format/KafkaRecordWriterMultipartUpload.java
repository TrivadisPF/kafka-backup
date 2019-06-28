package ch.tbd.kafka.backuprestore.backup.storage.format;

import ch.tbd.kafka.backuprestore.backup.kafkaconnect.compact.config.CompactBackupSinkConnectorConfig;
import ch.tbd.kafka.backuprestore.backup.kafkaconnect.config.BackupSinkConnectorConfig;
import ch.tbd.kafka.backuprestore.backup.storage.S3OutputStream;
import ch.tbd.kafka.backuprestore.model.avro.AvroKafkaRecord;
import ch.tbd.kafka.backuprestore.util.Constants;
import ch.tbd.kafka.backuprestore.util.SerializationDataUtils;
import com.amazonaws.services.s3.AmazonS3;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Class KafkaRecordWriterMultipartUpload.
 * This represents TODO.
 *
 * @author iorfinoa
 * @version $$Revision$$
 */
public class KafkaRecordWriterMultipartUpload implements RecordWriter {

    private Logger logger = LoggerFactory.getLogger(KafkaRecordWriterMultipartUpload.class);

    private BackupSinkConnectorConfig conf;
    private AmazonS3 amazonS3;
    private Schema schema;
    private S3OutputStream s3out;
    private DatumWriter<AvroKafkaRecord> writer = new GenericDatumWriter<AvroKafkaRecord>(AvroKafkaRecord.getClassSchema());
    private DataFileWriter<AvroKafkaRecord> dataFileWriter = new DataFileWriter(writer);


    public KafkaRecordWriterMultipartUpload(BackupSinkConnectorConfig connectorConfig, AmazonS3 amazonS3) {
        this.conf = connectorConfig;
        this.amazonS3 = amazonS3;
    }


    @Override
    public void write(SinkRecord sinkRecord) {
        if (schema == null) {
            schema = sinkRecord.valueSchema();
            try {
                s3out = new S3OutputStream(key(sinkRecord), conf, amazonS3);
                //TODO: Manage codec
                dataFileWriter.setCodec(CodecFactory.fromString("null"));
                dataFileWriter.create(AvroKafkaRecord.getClassSchema(), s3out);
            } catch (IOException e) {
                throw new ConnectException(e);
            }
        }
        logger.trace("Sink record: {}", sinkRecord);
        try {
            Map<CharSequence, ByteBuffer> newMapHeaders = new HashMap<>();

            Iterator<Header> it = sinkRecord.headers().iterator();
            while (it.hasNext()) {
                Header header = it.next();
                newMapHeaders.put(header.key(), ByteBuffer.wrap(SerializationDataUtils.serialize(header.value())));
            }
            AvroKafkaRecord avroKafkaRecord = AvroKafkaRecord.newBuilder()
                    .setTopic(sinkRecord.topic())
                    .setPartition(sinkRecord.kafkaPartition())
                    .setOffset(sinkRecord.kafkaOffset())
                    .setTimestamp(sinkRecord.timestamp())
                    .setKey(ByteBuffer.wrap(SerializationDataUtils.serialize(sinkRecord.key())))
                    .setValue(ByteBuffer.wrap(SerializationDataUtils.serialize(sinkRecord.value())))
                    .setHeaders(newMapHeaders)
                    .build();

            dataFileWriter.append(avroKafkaRecord);
        } catch (IOException e) {
            throw new ConnectException(e);
        }
    }

    @Override
    public void close() {
        try {
            dataFileWriter.close();
        } catch (IOException e) {
            throw new ConnectException(e);
        }
    }

    @Override
    public void commit() {
        try {
            dataFileWriter.flush();
            s3out.commit();
            dataFileWriter.close();
        } catch (IOException e) {
            throw new ConnectException(e);
        }
    }

    private String key(SinkRecord record) {
        if (conf instanceof CompactBackupSinkConnectorConfig) {
            //manage compacted log
            return String.format("%s/%s/%d/%s-%d-%s.avro", record.topic(), this.conf.getName(), record.kafkaPartition(),
                    record.topic(), record.kafkaPartition(), StringUtils.leftPad(String.valueOf(record.kafkaOffset()),
                            Constants.FIELD_INDEX_NAME_BACKUP, "0"));
        } else {
            return String.format("%s/%d/%s-%d-%s.avro", record.topic(), record.kafkaPartition(),
                    record.topic(), record.kafkaPartition(), StringUtils.leftPad(String.valueOf(record.kafkaOffset()),
                            Constants.FIELD_INDEX_NAME_BACKUP, "0"));
        }
    }

}
