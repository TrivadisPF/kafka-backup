package ch.tbd.kafka.backuprestore.restore.consumer_group;

/**
 * Class ValueConsumerGroup.
 * This represents TODO.
 *
 * @author iorfinoa
 * @version $$Revision$$
 */
public class ValueConsumerGroup {

    private short version;
    private long offset;
    private int leaderEpoch;
    private String metadata;
    private long commitTimestamp;
    private long expireTimestamp;

    public ValueConsumerGroup(short version, long offset, String metadata, long commitTimestamp) {
        this.version = version;
        this.offset = offset;
        this.metadata = metadata;
        this.commitTimestamp = commitTimestamp;
    }

    public ValueConsumerGroup(short version, long offset, String metadata, long commitTimestamp, long expireTimestamp) {
        this.version = version;
        this.offset = offset;
        this.metadata = metadata;
        this.commitTimestamp = commitTimestamp;
        this.expireTimestamp = expireTimestamp;
    }

    public ValueConsumerGroup(short version, long offset, int leaderEpoch, String metadata, long commitTimestamp) {
        this.version = version;
        this.offset = offset;
        this.leaderEpoch = leaderEpoch;
        this.metadata = metadata;
        this.commitTimestamp = commitTimestamp;
    }

    public short getVersion() {
        return version;
    }

    public long getOffset() {
        return offset;
    }

    public int getLeaderEpoch() {
        return leaderEpoch;
    }

    public String getMetadata() {
        return metadata;
    }

    public long getCommitTimestamp() {
        return commitTimestamp;
    }

    public long getExpireTimestamp() {
        return expireTimestamp;
    }
}
