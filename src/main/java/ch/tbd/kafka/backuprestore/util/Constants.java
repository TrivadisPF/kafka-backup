package ch.tbd.kafka.backuprestore.util;

import java.nio.charset.StandardCharsets;

/**
 * Class Constants.
 * This represents TODO.
 *
 * @author iorfinoa
 * @version $$Revision$$
 */
public interface Constants {

    public static final byte[] LINE_SEPARATOR_BYTES = System.lineSeparator().getBytes(StandardCharsets.UTF_8);

    public static final String PARTITION_ASSIGNED_KEY = "PARTITION_ASSIGNED_KEY";
}
