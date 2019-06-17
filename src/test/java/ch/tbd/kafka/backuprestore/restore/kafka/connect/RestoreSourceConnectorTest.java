package ch.tbd.kafka.backuprestore.restore.kafka.connect;

import ch.tbd.kafka.backuprestore.restore.kafkaconnect.RestoreSourceConnector;
import ch.tbd.kafka.backuprestore.restore.kafkaconnect.RestoreSourceTask;
import ch.tbd.kafka.backuprestore.restore.kafkaconnect.config.RestoreSourceConnectorConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Class RestoreSourceConnectorTest.
 * This represents TODO.
 *
 * @author iorfinoa
 * @version $$Revision$$
 */
public class RestoreSourceConnectorTest {

    @Test
    public void testTaskClass() {
        Assertions.assertEquals(RestoreSourceTask.class, new RestoreSourceConnector().taskClass());
    }

    @Test
    public void testConfig() {
        Assertions.assertEquals(RestoreSourceConnectorConfig.conf().getClass(), new RestoreSourceConnector().config().getClass());
    }
}
