package ch.tbd.kafka.backuprestore.util;

/**
 * Class Constants.
 * This represents TODO.
 *
 * @author iorfinoa
 * @version $$Revision$$
 */
public interface Constants {

    public static final int FIELD_INDEX_NAME_BACKUP = 15;

    public static final String S3_KEY_SEPARATOR = "/";
    public static final String DASH_KEY_SEPARATOR = "-";

    public static final String KEY_HEADER_RESTORED = "x-restored";
    public static final String KEY_HEADER_RECOVER = "x-recover";

    public static final String KEY_S3_TOPIC_NAME_TASK = "KEY_S3_TOPIC_NAME_TASK";
    public static final String KEY_TOPIC_NAME_TASK = "KEY_TOPIC_NAME_TASK";
    public static final String KEY_PARTITION_TASK = "KEY_PARTITION_TASK";
}
