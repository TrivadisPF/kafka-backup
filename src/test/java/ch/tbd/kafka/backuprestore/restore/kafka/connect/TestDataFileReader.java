package ch.tbd.kafka.backuprestore.restore.kafka.connect;

import ch.tbd.kafka.backuprestore.model.avro.AvroKafkaRecord;
import ch.tbd.kafka.backuprestore.restore.kafkaconnect.config.RestoreSourceConnectorConfig;
import ch.tbd.kafka.backuprestore.util.AmazonS3Utils;
import ch.tbd.kafka.backuprestore.util.SerializationDataUtils;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Class TestDataFileReader.
 * This represents TODO.
 *
 * @author iorfinoa
 * @version $$Revision$$
 */
public class TestDataFileReader {

    private final String S3_BUCKET_NAME = "TBD";
    private final String S3_REGION = "eu-central-1";
    private final String S3_TOPIC_NAME = "test-topic-2";
    private Logger logger = LoggerFactory.getLogger(TestDataFileReader.class);
    private AmazonS3 amazonS3;

    @BeforeEach
    public void init() {
        Map<String, String> map = new HashMap<>();
        map.put(RestoreSourceConnectorConfig.S3_REGION_CONFIG, "eu-central-1");
        RestoreSourceConnectorConfig connectorConfig = new RestoreSourceConnectorConfig(map);
        amazonS3 = AmazonS3Utils.initConnection(connectorConfig);
    }

    @AfterEach
    public void end() {

    }

    @Test
    public void start() {

        ListObjectsV2Request request = new ListObjectsV2Request();
        request.setBucketName(S3_BUCKET_NAME);
        request.withPrefix(S3_TOPIC_NAME + "/0/");
        ListObjectsV2Result result = amazonS3.listObjectsV2(request);
        List<S3ObjectSummary> s3ObjectSummaryList = result.getObjectSummaries();
        s3ObjectSummaryList.forEach(s3ObjectSummary -> {
            GetObjectRequest getObjectRequest = new GetObjectRequest(S3_BUCKET_NAME, s3ObjectSummary.getKey());
            InputStream io = amazonS3.getObject(getObjectRequest).getObjectContent();
            try {
                DatumReader<AvroKafkaRecord> reader = new GenericDatumReader<>(AvroKafkaRecord.getClassSchema());
                DataFileStream<AvroKafkaRecord> objectDataFileStream = new DataFileStream<>(io, reader);
                while (objectDataFileStream.hasNext()) {
                    AvroKafkaRecord record = new AvroKafkaRecord();
                    objectDataFileStream.next(record);
                    System.out.println(SerializationDataUtils.deserialize(record.getKey().array()));
                    String value = new String(record.getValue().array());
                    System.out.println(value);
                    System.out.println(record.toString());
                }

            } catch (IOException e) {
                e.printStackTrace();
            }
        });

    }
}
