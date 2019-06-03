package ch.tbd.kafka.backuprestore.util;

/**
 * Class StringUtils.
 * This represents TODO.
 *
 * @author iorfinoa
 * @version $$Revision$$
 */
public class StringUtils {
    public static boolean isBlank(String string) {
        return string == null || string.isEmpty() || string.trim().isEmpty();
    }

    public static boolean isNotBlank(String string) {
        return !isBlank(string);
    }
}
