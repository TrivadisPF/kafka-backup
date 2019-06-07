package ch.tbd.kafka.backuprestore.backup.storage.format;

import ch.tbd.kafka.backuprestore.backup.kafkaconnect.BackupSinkConnectorConfig;
import ch.tbd.kafka.backuprestore.backup.serializers.KafkaRecordSerializer;
import ch.tbd.kafka.backuprestore.backup.serializers.avro.KafkaRecordAvroSerializer;
import ch.tbd.kafka.backuprestore.backup.storage.S3OutputStream;
import ch.tbd.kafka.backuprestore.model.KafkaRecord;
import ch.tbd.kafka.backuprestore.model.avro.AvroKafkaRecord;
import ch.tbd.kafka.backuprestore.util.AmazonS3Utils;
import ch.tbd.kafka.backuprestore.util.Constants;
import com.amazonaws.services.s3.AmazonS3;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.springframework.util.SerializationUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

/**
 * Class KafkaRecordWriterMultipartUpload.
 * This represents TODO.
 *
 * @author iorfinoa
 * @version $$Revision$$
 */
public class KafkaRecordWriterMultipartUpload implements RecordWriter {

    private BackupSinkConnectorConfig conf;
    private AmazonS3 amazonS3;
    private String bucket;
    private S3OutputStream s3out;
    private OutputStream s3outWrapper;
    private KafkaRecordSerializer kafkaRecordSerializer = new KafkaRecordAvroSerializer();


    public KafkaRecordWriterMultipartUpload(BackupSinkConnectorConfig connectorConfig) {
        this.conf = connectorConfig;
        /*
        AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();
        builder.withRegion(connectorConfig.getRegionConfig());
        builder.withCredentials(new ProfileCredentialsProvider());
        if (connectorConfig.getProxyUrlConfig() != null && !connectorConfig.getProxyUrlConfig().isEmpty() && connectorConfig.getProxyPortConfig() > 0) {
            ClientConfiguration config = new ClientConfiguration();
            config.setProtocol(Protocol.HTTPS);
            config.setProxyHost(connectorConfig.getProxyUrlConfig());
            config.setProxyPort(connectorConfig.getProxyPortConfig());
            builder.withClientConfiguration(config);
        }*/
        this.amazonS3 = AmazonS3Utils.initConnection(connectorConfig.getRegionConfig(),
                connectorConfig.getProxyUrlConfig(), connectorConfig.getProxyPortConfig());
        this.bucket = connectorConfig.getBucketName();
    }


    @Override
    public void write(SinkRecord sinkRecord) {
        KafkaRecord record = new KafkaRecord(sinkRecord.topic(), sinkRecord.kafkaPartition(), sinkRecord.kafkaOffset(), sinkRecord.timestamp(),
                //        getBuffer(sinkRecord.key()), getBuffer(sinkRecord.value()));
                ByteBuffer.wrap((byte[]) sinkRecord.key()), ByteBuffer.wrap((byte[]) sinkRecord.value()));

        if (s3outWrapper == null) {
            s3out = new S3OutputStream(key(record), conf, amazonS3);
            this.s3outWrapper = s3out.wrapForCompression();
        }
        try {
/*
            AvroKafkaRecord avroKafkaRecord = AvroKafkaRecord.newBuilder()
                    .setTopic(record.getTopic())
                    .setPartition(record.getPartition())
                    .setOffset(record.getOffset())
                    .setTimestamp(record.getTimestamp())
                    .setKey(record.getKey())
                    .setValue(record.getValue())
                    .setHeaders(adaptHeaders(record.getHeaders()))
                    .build();
*/
            if (sinkRecord.headers() != null) {
                sinkRecord.headers().forEach(header ->
                        record.addHeader(header.key(), SerializationUtils.serialize(header.value()))
                );
            }
            //s3outWrapper.write(kafkaRecordSerializer.serialize(record).array());
            s3outWrapper.write(Base64.getEncoder().encode(kafkaRecordSerializer.serialize(record).array()));
            //s3outWrapper.write(serialize(avroKafkaRecord));
            s3outWrapper.write(Constants.LINE_SEPARATOR_BYTES);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void close() {

    }

    @Override
    public void commit() {
        try {
            s3out.commit();
            s3outWrapper.close();
        } catch (IOException e) {
            throw new ConnectException(e);
        }
    }


    public ByteBuffer getBuffer(Object obj) {
        ObjectOutputStream ostream;
        ByteArrayOutputStream bstream = new ByteArrayOutputStream();

        try {
            ostream = new ObjectOutputStream(bstream);
            ostream.writeObject(obj);
            ByteBuffer buffer = ByteBuffer.allocate(bstream.size());
            buffer.put(bstream.toByteArray());
            ((Buffer) buffer).flip();
            return buffer;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private String key(KafkaRecord record) {
        return String.format("%s/%d/index-%d.avro-messages", record.getTopic(), record.getPartition(), record.getOffset());
    }


    private byte[] serialize(AvroKafkaRecord record) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream obj = new ObjectOutputStream(baos);
            obj.writeObject(record);
            byte[] bytes = baos.toByteArray();
            obj.close();
            baos.close();
            return bytes;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
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
