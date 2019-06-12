package ch.tbd.kafka.backuprestore.backup.writers.s3;

import ch.tbd.kafka.backuprestore.backup.serializers.KafkaRecordSerializer;
import ch.tbd.kafka.backuprestore.backup.writers.AbstractKafkaRecordWriter;
import ch.tbd.kafka.backuprestore.model.KafkaRecord;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;

//TODO: write more records in one file (one file for time unit?)
//TODO: flush
//TODO: Old class. Remove it?
@Deprecated
public class S3KafkaRecordWriter extends AbstractKafkaRecordWriter {

    private AmazonS3 amazonS3;
    private String bucket;

    public S3KafkaRecordWriter(String region, String bucket, String proxyHost, int proxyPort, KafkaRecordSerializer kafkaRecordSerializer) {
        super(kafkaRecordSerializer);
        AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();
        builder.withRegion(region);
        builder.withCredentials(new ProfileCredentialsProvider());
        if (proxyHost != null && !proxyHost.isEmpty() && proxyPort > 0) {
            ClientConfiguration config = new ClientConfiguration();
            config.setProtocol(Protocol.HTTPS);
            config.setProxyHost(proxyHost);
            config.setProxyPort(proxyPort);
            builder.withClientConfiguration(config);
        }
        this.amazonS3 = builder.build();
        this.bucket = bucket;
    }

    @Override
    public void write(List<KafkaRecord> records) {
        records.forEach(record -> write(record));
    }

    private void write(KafkaRecord record) {
        ByteBuffer byteBuffer = kafkaRecordSerializer.serialize(record);
        ObjectMetadata metadata = new ObjectMetadata();
        //TODO: how does expiration work on S3. Could it be an implementation for retention policy?
//        metadata.setExpirationTime();
        metadata.setContentType("application/octet-stream");
        metadata.addUserMetadata("x-topic", record.getTopic());
        metadata.addUserMetadata("x-partition", Integer.toString(record.getPartition()));
        metadata.addUserMetadata("x-offset", Long.toString(record.getOffset()));
        metadata.addUserMetadata("x-timestamp", Long.toString(record.getTimestamp()));
        PutObjectRequest request = new PutObjectRequest(bucket, key(record), new ByteBufferBackedInputStream(byteBuffer), metadata);
        amazonS3.putObject(request);
    }

    private String key(KafkaRecord record) {
        return String.format("%s/%d/%d", record.getTopic(), record.getPartition(), record.getOffset());
    }

}
