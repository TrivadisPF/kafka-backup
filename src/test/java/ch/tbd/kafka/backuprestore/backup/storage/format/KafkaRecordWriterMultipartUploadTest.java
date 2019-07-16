package ch.tbd.kafka.backuprestore.backup.storage.format;

import ch.tbd.kafka.backuprestore.backup.kafkaconnect.config.BackupSinkConnectorConfig;
import com.amazonaws.services.s3.AmazonS3;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

/**
 * Class KafkaRecordWriterMultipartUploadTest.
 * This represents TODO.
 *
 * @author iorfinoa
 * @version $$Revision$$
 */
public class KafkaRecordWriterMultipartUploadTest {

    private KafkaRecordWriterMultipartUpload kafkaRecordWriterMultipartUpload;

    @Mock
    private AmazonS3 amazonS3;

    @Mock
    private BackupSinkConnectorConfig backupSinkConnectorConfig;

    public void init() {
        this.kafkaRecordWriterMultipartUpload = new KafkaRecordWriterMultipartUpload(backupSinkConnectorConfig, amazonS3);
    }

    @Test
    public void testWrite() {
        SinkRecord sinkRecord = new SinkRecord("topic-name", 0, Schema.STRING_SCHEMA, "KEY", Schema.STRING_SCHEMA, "VALUE", 0);


    }
}
