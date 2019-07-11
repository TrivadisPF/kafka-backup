package ch.tbd.kafka.backuprestore.backup.kafkaconnect.retention;

import ch.tbd.kafka.backuprestore.AbstractTest;
import ch.tbd.kafka.backuprestore.backup.kafkaconnect.config.BackupSinkConnectorConfig;
import ch.tbd.kafka.backuprestore.backup.storage.S3OutputStream;
import ch.tbd.kafka.backuprestore.util.AmazonS3Utils;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.BucketLifecycleConfiguration;
import com.amazonaws.services.s3.model.lifecycle.LifecycleFilter;
import com.amazonaws.services.s3.model.lifecycle.LifecyclePrefixPredicate;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Class RetentionTest.
 * This represents TODO.
 *
 * @author iorfinoa
 * @version $$Revision$$
 */
public class RetentionTest extends AbstractTest {

    private Logger logger = LoggerFactory.getLogger(RetentionTest.class);

    private String prefix;
    private String nameObject = "objectName";

    private AmazonS3 amazonS3;
    private BackupSinkConnectorConfig conf;


    @Override
    protected List<String> getListPropertyFiles() {
        return Arrays.asList("/RetentionTest.properties");
    }

    @BeforeEach
    public void init() {
        conf = new BackupSinkConnectorConfig(getPropertiesMap());
        amazonS3 = AmazonS3Utils.initConnection(conf);
        this.prefix = getPropertiesMap().get(PREFIX_KEY);
    }

    @Test
    public void uploadFile() {
        String idRule = "id-test-rule";
        BucketLifecycleConfiguration.Rule rule = new BucketLifecycleConfiguration.Rule()
                .withId(idRule)
                .withFilter(new LifecycleFilter((new LifecyclePrefixPredicate(this.prefix))))
                .withExpirationInDays(1)
                .withStatus(BucketLifecycleConfiguration.ENABLED);

        BucketLifecycleConfiguration configuration = amazonS3.getBucketLifecycleConfiguration(getBucketName());
        if (configuration == null) {
            configuration = new BucketLifecycleConfiguration();
        }
        List<BucketLifecycleConfiguration.Rule> rules = new ArrayList<>();
        rules.add(rule);
        if (configuration.getRules() != null) {
            for (BucketLifecycleConfiguration.Rule ruleTmp : configuration.getRules()) {
                if (!ruleTmp.getId().equalsIgnoreCase(idRule)) {
                    rules.add(ruleTmp);
                }
            }
        }
        configuration.setRules(rules);

        amazonS3.setBucketLifecycleConfiguration(getBucketName(), configuration);
        BucketLifecycleConfiguration bucketLifecycleConfiguration = amazonS3.getBucketLifecycleConfiguration(getBucketName());
        Assertions.assertEquals(rules.size(), bucketLifecycleConfiguration.getRules().size());
        if (bucketLifecycleConfiguration == null || bucketLifecycleConfiguration.getRules() == null
                || bucketLifecycleConfiguration.getRules().isEmpty()) {
            boolean found = false;
            for (BucketLifecycleConfiguration.Rule ruleRead : bucketLifecycleConfiguration.getRules()) {
                if (ruleRead.getId().equalsIgnoreCase(idRule)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                Assertions.fail("Configuration found is not expected");
            }
        }

        S3OutputStream s3out = null;
        DatumWriter<String> writer = null;
        DataFileWriter<String> dataFileWriter = null;
        try {
            s3out = new S3OutputStream(this.prefix + this.nameObject, conf, amazonS3);
            writer = new GenericDatumWriter<String>(Schema.create(Schema.Type.STRING));
            dataFileWriter = new DataFileWriter(writer);
            dataFileWriter.create(Schema.create(Schema.Type.STRING), s3out);

            dataFileWriter.append("test1");
            dataFileWriter.append("test2");
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            Assertions.assertTrue(false);
        } finally {
            if (dataFileWriter != null) {
                try {
                    dataFileWriter.flush();
                    s3out.commit();
                    dataFileWriter.close();
                } catch (IOException e) {
                    logger.error(e.getMessage(), e);
                    Assertions.assertTrue(false);
                }
            }
            if (s3out != null) {
                try {
                    s3out.flush();
                    s3out.close();
                } catch (IOException e) {
                    logger.error(e.getMessage(), e);
                    Assertions.assertTrue(false);
                }
            }
        }
        Assertions.assertTrue(true);


    }

}
