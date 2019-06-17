package ch.tbd.kafka.backuprestore.backup.kafkaconnect;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkTaskContext;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Class MockSinkTaskContext.
 * This represents TODO.
 *
 * @author iorfinoa
 * @version $$Revision$$
 */
public class MockSinkTaskContext implements SinkTaskContext {

    private final Map<TopicPartition, Long> offsets;
    private long timeoutMs;
    private Set<TopicPartition> assignment;

    public MockSinkTaskContext(Set<TopicPartition> assignment) {
        this.offsets = new HashMap<>();
        this.timeoutMs = -1L;
        this.assignment = assignment;
    }

    @Override
    public Map<String, String> configs() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void offset(Map<TopicPartition, Long> offsets) {
        this.offsets.putAll(offsets);
    }

    @Override
    public void offset(TopicPartition tp, long offset) {
        offsets.put(tp, offset);
    }

    public Map<TopicPartition, Long> offsets() {
        return offsets;
    }

    @Override
    public void timeout(long timeoutMs) {
        this.timeoutMs = timeoutMs;
    }

    public long timeout() {
        return timeoutMs;
    }

    @Override
    public Set<TopicPartition> assignment() {
        return assignment;
    }

    public void setAssignment(Set<TopicPartition> nextAssignment) {
        assignment = nextAssignment;
    }

    @Override
    public void pause(TopicPartition... partitions) {
    }

    @Override
    public void resume(TopicPartition... partitions) {
    }

    @Override
    public void requestCommit() {
    }

}
