package ch.tbd.kafka.backuprestore.util;

import java.io.Serializable;
import java.util.Objects;

/**
 * Class IndexObj.
 * This represents TODO.
 *
 * @author iorfinoa
 * @version $$Revision$$
 */
public class IndexObj implements Serializable {

    private int partition;
    private long offset;

    public IndexObj(int partition, long offset) {
        this.partition = partition;
        this.offset = offset;
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IndexObj indexObj = (IndexObj) o;
        return partition == indexObj.partition &&
                offset == indexObj.offset;
    }

    @Override
    public int hashCode() {
        return Objects.hash(partition, offset);
    }
}
