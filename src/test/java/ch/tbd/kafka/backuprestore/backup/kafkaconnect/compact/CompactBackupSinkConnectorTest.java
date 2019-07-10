package ch.tbd.kafka.backuprestore.backup.kafkaconnect.compact;

import ch.tbd.kafka.backuprestore.backup.kafkaconnect.compact.config.CompactBackupSinkConnectorConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Class CompactBackupSinkConnectorTest.
 * This represents TODO.
 *
 * @author iorfinoa
 * @version $$Revision$$
 */
public class CompactBackupSinkConnectorTest {

    @Test
    public void testTaskClass() {
        Assertions.assertEquals(CompactBackupSinkTask.class, new CompactBackupSinkConnector().taskClass());
    }

    @Test
    public void testConfig() {
        Assertions.assertEquals(CompactBackupSinkConnectorConfig.conf().getClass(), new CompactBackupSinkConnector().config().getClass());
    }
}
