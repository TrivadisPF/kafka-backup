package ch.tbd.kafka.backuprestore.restore.readers.s3;

import ch.tbd.kafka.backuprestore.model.KafkaRecord;
import ch.tbd.kafka.backuprestore.restore.deserializers.KafkaRecordDeserializer;
import ch.tbd.kafka.backuprestore.restore.readers.AbstractKafkaRecordReader;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.util.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class S3KafkaRecordReader extends AbstractKafkaRecordReader {

    private static final Logger LOGGER = LoggerFactory.getLogger(S3KafkaRecordReader.class);

    private AmazonS3 amazonS3;
    private String bucket;

    public S3KafkaRecordReader(String region, String bucket, KafkaRecordDeserializer kafkaRecordDeserializer) {
        super(kafkaRecordDeserializer);
        this.amazonS3 = AmazonS3ClientBuilder.standard()
                .withRegion(region)
                .withCredentials(new ProfileCredentialsProvider())
                .build();
        this.bucket = bucket;
    }

    @Override
    public List<KafkaRecord> read(String topic) {
        ObjectListing objects = amazonS3.listObjects(bucket, topic);
        List<S3ObjectSummary> summaryList = objects.getObjectSummaries();
        List<KafkaRecord> records = new ArrayList<>();
        summaryList.forEach(s3ObjectSummary -> {
            records.add(
                    read(
                            topic(s3ObjectSummary.getKey()),
                            partition(s3ObjectSummary.getKey()),
                            offset(s3ObjectSummary.getKey())
                    ));
        });
        return records;
    }

    @Override
    public List<KafkaRecord> read(String topic, int partition) {
        return null;
    }

    @Override
    public KafkaRecord read(String topic, int partition, long offset) {
        try {
            S3Object object = amazonS3.getObject(bucket, key(topic, partition, offset));
            if (object == null) {
                return null;
            }
            //TODO Streaming??
            InputStream is = object.getObjectContent().getDelegateStream();
            return kafkaRecordDeserializer.deserialize(ByteBuffer.wrap(IOUtils.toByteArray(is)));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<KafkaRecord> readBetweenTimestamps(String topic, int partition, long from, long to) {
        return null;
    }

    @Override
    public List<KafkaRecord> readBetweenOffsets(String topic, int partition, long from, long to) {
        return null;
    }

    private String topic(String key) {
        return key.substring(0, key.indexOf("/"));
    }

    private int partition(String key) {
        return Integer.parseInt(key.substring(key.indexOf("/") + 1, key.lastIndexOf("/")));
    }

    private long offset(String key) {
        return Long.parseLong(key.substring(key.lastIndexOf("/") + 1, key.length()));
    }

    private String key(String topic, int partition, long offset) {
        return String.format("%s/%d/%d", topic, partition, offset);
    }

}
