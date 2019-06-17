package ch.tbd.kafka.backuprestore.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Class StringUtilsTest.
 * This represents TODO.
 *
 * @author iorfinoa
 * @version $$Revision$$
 */
public class StringUtilsTest {

    @Test
    public void testIsBlank() {
        Assertions.assertTrue(StringUtils.isBlank(null));
        Assertions.assertTrue(StringUtils.isBlank(""));
        Assertions.assertTrue(StringUtils.isBlank(" "));
        Assertions.assertFalse(StringUtils.isBlank("TEST"));
    }

    @Test
    public void testIsNotBlank() {
        Assertions.assertFalse(StringUtils.isNotBlank(null));
        Assertions.assertFalse(StringUtils.isNotBlank(""));
        Assertions.assertFalse(StringUtils.isNotBlank(" "));
        Assertions.assertTrue(StringUtils.isNotBlank("TEST"));
    }
}
