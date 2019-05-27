package ch.tbd.kafka.backuprestore.backup.storage.format;

import ch.tbd.kafka.backuprestore.backup.kafkaconnect.BackupSinkConnectorConfig;
import ch.tbd.kafka.backuprestore.model.KafkaRecord;
import ch.tbd.kafka.backuprestore.model.avro.AvroKafkaRecord;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import org.apache.kafka.connect.sink.SinkRecord;
import org.springframework.util.SerializationUtils;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Class KafkaRecordWriter.
 * This represents TODO.
 *
 * @author iorfinoa
 * @version $$Revision$$
 */
public class KafkaRecordWriter implements RecordWriter {

    private LinkedList<KafkaRecord> records = new LinkedList<>();
    private AmazonS3 amazonS3;
    private String bucket;

    public KafkaRecordWriter(BackupSinkConnectorConfig connectorConfig) {
        AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();
        builder.withRegion(connectorConfig.getRegionConfig());
        builder.withCredentials(new ProfileCredentialsProvider());
        if (connectorConfig.getProxyUrlConfig() != null && !connectorConfig.getProxyUrlConfig().isEmpty() && connectorConfig.getProxyPortConfig() > 0) {
            ClientConfiguration config = new ClientConfiguration();
            config.setProtocol(Protocol.HTTPS);
            config.setProxyHost(connectorConfig.getProxyUrlConfig());
            config.setProxyPort(connectorConfig.getProxyPortConfig());
            builder.withClientConfiguration(config);
        }
        this.amazonS3 = builder.build();
        this.bucket = connectorConfig.getBucketName();
    }


    @Override
    public void write(SinkRecord sinkRecord) {
        KafkaRecord record = new KafkaRecord(sinkRecord.topic(), sinkRecord.kafkaPartition(), sinkRecord.kafkaOffset(), sinkRecord.timestamp(), getBuffer(sinkRecord.key()), getBuffer(sinkRecord.value()));
        if (sinkRecord.headers() != null) {
            sinkRecord.headers().forEach(header ->
                    record.addHeader(header.key(), SerializationUtils.serialize(header.value()))
            );
        }
        records.add(record);
    }

    @Override
    public void close() {

    }

    @Override
    public void commit() {
        InputStream byteBuffer = serialize(records);
        ObjectMetadata metadata = new ObjectMetadata();
        //TODO: how does expiration work on S3. Could it be an implementation for retention policy?
//        metadata.setExpirationTime();
        // TODO: How to set the metadata for entire block? I can use the last record?

        KafkaRecord record = records.getLast();

        metadata.setContentType("application/octet-stream");
        metadata.addUserMetadata("x-topic", record.getTopic());
        metadata.addUserMetadata("x-partition", Integer.toString(record.getPartition()));
        metadata.addUserMetadata("x-offset", Long.toString(record.getOffset()));
        metadata.addUserMetadata("x-timestamp", Long.toString(record.getTimestamp()));

        PutObjectRequest request = new PutObjectRequest(bucket, key(records), byteBuffer, metadata);
        amazonS3.putObject(request);
    }


    private InputStream serialize(List<KafkaRecord> records) {
        byte[] byteArrayFinal = null;
        AvroKafkaRecord[] records1 = new AvroKafkaRecord[records.size()];
        int pos = 0;

        for (KafkaRecord record : records) {
            AvroKafkaRecord avroKafkaRecord = AvroKafkaRecord.newBuilder()
                    .setTopic(record.getTopic())
                    .setPartition(record.getPartition())
                    .setOffset(record.getOffset())
                    .setTimestamp(record.getTimestamp())
                    .setKey(record.getKey())
                    .setValue(record.getValue())
                    .setHeaders(adaptHeaders(record.getHeaders()))
                    .build();
            records1[pos] = avroKafkaRecord;
            try {
                byte[] byteArrayRecord = avroKafkaRecord.toByteBuffer().array();
                if (byteArrayFinal == null) {
                    byteArrayFinal = byteArrayRecord;
                } else {
                    int len = byteArrayFinal.length;
                    byte[] byteArrayTmp = byteArrayFinal;
                    byteArrayFinal = new byte[len + byteArrayRecord.length];
                    System.arraycopy(byteArrayTmp, 0, byteArrayFinal, 0, byteArrayTmp.length);
                    System.arraycopy(byteArrayRecord, 0, byteArrayFinal, 0, byteArrayRecord.length);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            pos++;
        }
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream obj = new ObjectOutputStream(baos);
            obj.writeObject(records1);
            byte[] bytes = baos.toByteArray();
            obj.close();
            baos.close();
            return bytes != null ? new ByteArrayInputStream(bytes) : null;

        }catch (Exception e) {
            e.printStackTrace();
        }
        return byteArrayFinal != null ? new ByteArrayInputStream(byteArrayFinal) : null;
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

    public ByteBuffer getBuffer(Object obj) {
        ObjectOutputStream ostream;
        ByteArrayOutputStream bstream = new ByteArrayOutputStream();

        try {
            ostream = new ObjectOutputStream(bstream);
            ostream.writeObject(obj);
            ByteBuffer buffer = ByteBuffer.allocate(bstream.size());
            buffer.put(bstream.toByteArray());
            buffer.flip();
            return buffer;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private String key(LinkedList<KafkaRecord> records) {
        KafkaRecord record = records.getLast();
        return String.format("%s/%d/index-%d.avro-messages", record.getTopic(), record.getPartition(), record.getOffset());
    }
}
