package ch.tbd.kafka.backuprestore.backup.storage.partitioner;

import ch.tbd.kafka.backuprestore.backup.kafkaconnect.BackupSinkConnectorConfig;
import ch.tbd.kafka.backuprestore.backup.storage.format.KafkaRecordWriter;
import ch.tbd.kafka.backuprestore.backup.storage.format.KafkaRecordWriterMultipartUpload;
import ch.tbd.kafka.backuprestore.backup.storage.format.RecordWriter;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.IllegalWorkerStateException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.errors.SchemaProjectorException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Class TopicPartitionWriter.
 * This represents TODO.
 *
 * @author iorfinoa
 * @version $$Revision$$
 */
public class TopicPartitionWriter {
    private static final Logger log = LoggerFactory.getLogger(TopicPartitionWriter.class);
    private final Map<String, RecordWriter> writers;
    private final Map<String, Schema> currentSchemas;
    private final TopicPartition tp;
    private final Partitioner<?> partitioner;
    private State state;
    private final Queue<SinkRecord> buffer;
    private final SinkTaskContext context;
    private int recordCount;
    private final int flushSize;
    private final long rotateIntervalMs;
    private long currentOffset;
    private Long currentTimestamp;
    private String currentEncodedPartition;
    private Long baseRecordTimestamp;
    private Long offsetToCommit;
    private final Map<String, Long> startOffsets;
    private long timeoutMs;
    private long failureTime;
    private final Time time;
    private DateTimeZone timeZone;
    private final BackupSinkConnectorConfig connectorConfig;

    public TopicPartitionWriter(TopicPartition tp,
                                BackupSinkConnectorConfig connectorConfig,
                                SinkTaskContext context,
                                Partitioner<?> partitioner,
                                Time time) {
        this.partitioner = partitioner;
        this.tp = tp;
        this.context = context;
        this.connectorConfig = connectorConfig;
        this.time = time;
        buffer = new LinkedList<>();
        writers = new HashMap<>();
        currentSchemas = new HashMap<>();
        startOffsets = new HashMap<>();
        state = State.WRITE_STARTED;
        failureTime = -1L;
        currentOffset = -1L;

        this.flushSize = this.connectorConfig.getInt(BackupSinkConnectorConfig.FLUSH_SIZE_CONFIG);
        rotateIntervalMs = this.connectorConfig.getLong(BackupSinkConnectorConfig.ROTATE_INTERVAL_MS_CONFIG);
        timeoutMs = connectorConfig.getLong(BackupSinkConnectorConfig.RETRY_BACKOFF_CONFIG);
    }

    // Visible for testing
    TopicPartitionWriter(TopicPartition tp,
                         Partitioner<?> partitioner,
                         BackupSinkConnectorConfig connectorConfig,
                         SinkTaskContext context,
                         Time time) {
        this.connectorConfig = connectorConfig;
        this.time = time;
        this.tp = tp;
        this.context = context;
        this.partitioner = partitioner;
        flushSize = connectorConfig.getInt(BackupSinkConnectorConfig.FLUSH_SIZE_CONFIG);
        rotateIntervalMs = connectorConfig.getLong(BackupSinkConnectorConfig.ROTATE_INTERVAL_MS_CONFIG);
        timeoutMs = connectorConfig.getLong(BackupSinkConnectorConfig.RETRY_BACKOFF_CONFIG);
        buffer = new LinkedList<>();
        writers = new HashMap<>();
        currentSchemas = new HashMap<>();
        startOffsets = new HashMap<>();
        state = State.WRITE_STARTED;
        failureTime = -1L;
        currentOffset = -1L;
    }

    private enum State {
        WRITE_STARTED,
        WRITE_PARTITION_PAUSED,
        SHOULD_ROTATE,
        FILE_COMMITTED;

        private static final State[] VALS = values();

        public State next() {
            return VALS[(ordinal() + 1) % VALS.length];
        }
    }

    public void write() {
        long now = time.milliseconds();
        if (failureTime > 0 && now - failureTime < timeoutMs) {
            return;
        }

        while (!buffer.isEmpty()) {
            try {
                executeState(now);
            } catch (SchemaProjectorException | IllegalWorkerStateException e) {
                throw new ConnectException(e);
            } catch (RetriableException e) {
                log.error("Exception on topic partition {}: ", tp, e);
                failureTime = time.milliseconds();
                setRetryTimeout(timeoutMs);
                break;
            }
        }
        commitOnTimeIfNoData(now);
    }

    @SuppressWarnings("fallthrough")
    private void executeState(long now) {
        switch (state) {
            case WRITE_STARTED:
                pause();
                nextState();
                // fallthrough
            case WRITE_PARTITION_PAUSED:

                SinkRecord record = buffer.peek();
                currentTimestamp = new Date().getTime();
                if (baseRecordTimestamp == null) {
                    baseRecordTimestamp = currentTimestamp;
                }
                String encodedPartition = partitioner.encodePartition(record);

                if (!checkRotationOrAppend(
                        record,
                        encodedPartition,
                        now
                )) {
                    break;
                }
                // fallthrough
            case SHOULD_ROTATE:
                commitFiles();
                nextState();
                // fallthrough
            case FILE_COMMITTED:
                setState(State.WRITE_PARTITION_PAUSED);
                break;
            default:
                log.error("{} is not a valid state to write record for topic partition {}.", state, tp);
        }
    }

    private boolean checkRotationOrAppend(
            SinkRecord record,
            String encodedPartition,
            long now
    ) {
        if (rotateOnTime(encodedPartition, currentTimestamp, now)) {
            nextState();
        } else {
            currentEncodedPartition = encodedPartition;
            writeRecord(record);
            buffer.poll();
            if (rotateOnSize()) {
                log.info(
                        "Starting commit and rotation for topic partition {} with start offset {}",
                        tp,
                        startOffsets
                );
                nextState();
                // Fall through and try to rotate immediately
            } else {
                return false;
            }
        }
        return true;
    }

    private void commitOnTimeIfNoData(long now) {
        if (buffer.isEmpty()) {
            // committing files after waiting for rotateIntervalMs time but less than flush.size
            // records available
            if (recordCount > 0 && rotateOnTime(currentEncodedPartition, currentTimestamp, now)) {
                log.info(
                        "Committing files after waiting for rotateIntervalMs time but less than flush.size "
                                + "records available."
                );

                try {
                    commitFiles();
                } catch (ConnectException e) {
                    log.error("Exception on topic partition {}: ", tp, e);
                    failureTime = time.milliseconds();
                    setRetryTimeout(timeoutMs);
                }
            }

            resume();
            setState(State.WRITE_STARTED);
        }
    }

    public void close() throws ConnectException {
        log.debug("Closing TopicPartitionWriter {}", tp);
        for (RecordWriter writer : writers.values()) {
            writer.close();
        }
        writers.clear();
        startOffsets.clear();
    }

    public void buffer(SinkRecord sinkRecord) {
        buffer.add(sinkRecord);
    }

    public Long getOffsetToCommitAndReset() {
        Long latest = offsetToCommit;
        offsetToCommit = null;
        return latest;
    }

    private void nextState() {
        state = state.next();
    }

    private void setState(State state) {
        this.state = state;
    }

    private boolean rotateOnTime(String encodedPartition, Long recordTimestamp, long now) {
        if (recordCount <= 0) {
            return false;
        }
        boolean periodicRotation = rotateIntervalMs > 0
                && (
                recordTimestamp - baseRecordTimestamp >= rotateIntervalMs
                        || !encodedPartition.equals(currentEncodedPartition)
        );

        log.trace(
                "Checking rotation on time with recordCount '{}' and encodedPartition '{}'",
                recordCount,
                encodedPartition
        );

        log.trace(
                "Should apply periodic time-based rotation (rotateIntervalMs: '{}', baseRecordTimestamp: "
                        + "'{}', timestamp: '{}', encodedPartition: '{}', currentEncodedPartition: '{}')? {}",
                rotateIntervalMs,
                baseRecordTimestamp,
                recordTimestamp,
                encodedPartition,
                currentEncodedPartition,
                periodicRotation
        );


        return periodicRotation;
    }

    private boolean rotateOnSize() {
        boolean messageSizeRotation = recordCount >= flushSize;
        log.trace(
                "Should apply size-based rotation (count {} >= flush size {})? {}",
                recordCount,
                flushSize,
                messageSizeRotation
        );
        return messageSizeRotation;
    }

    private void pause() {
        log.trace("Pausing writer for topic-partition '{}'", tp);
        context.pause(tp);
    }

    private void resume() {
        log.trace("Resuming writer for topic-partition '{}'", tp);
        context.resume(tp);
    }

    private RecordWriter getWriter(SinkRecord record, String encodedPartition)
            throws ConnectException {
        if (writers.containsKey(encodedPartition)) {
            return writers.get(encodedPartition);
        }
        //String commitFilename = getCommitFilename(encodedPartition);

        // TODO: Optimize how to extract an instance of RecordWriter
        //RecordWriter writer = new KafkaRecordWriter(connectorConfig);
        RecordWriter writer = new KafkaRecordWriterMultipartUpload(connectorConfig);
        writers.put(encodedPartition, writer);
        return writer;
    }

    private void writeRecord(SinkRecord record) {
        currentOffset = record.kafkaOffset();

        if (!startOffsets.containsKey(currentEncodedPartition)) {
            log.trace(
                    "Setting writer's start offset for '{}' to {}",
                    currentEncodedPartition,
                    currentOffset
            );
            startOffsets.put(currentEncodedPartition, currentOffset);
        }

        RecordWriter writer = getWriter(record, currentEncodedPartition);
        writer.write(record);
        ++recordCount;
    }

    private void commitFiles() {
        for (Map.Entry<String, RecordWriter> entry : writers.entrySet()) {
            commitFile(entry.getKey());
            log.debug("Committed {} for {}", entry.getValue(), tp);
        }
        offsetToCommit = currentOffset + 1;
        currentSchemas.clear();
        recordCount = 0;
        baseRecordTimestamp = null;
        log.info("Files committed to S3. Target commit offset for {} is {}", tp, offsetToCommit);
    }

    private void commitFile(String encodedPartition) {
        if (!startOffsets.containsKey(encodedPartition)) {
            log.warn("Tried to commit file with missing starting offset partition: {}. Ignoring.");
            return;
        }

        if (writers.containsKey(encodedPartition)) {
            RecordWriter writer = writers.get(encodedPartition);
            // Commits the file and closes the underlying output stream.
            writer.commit();
            writers.remove(encodedPartition);
            log.debug("Removed writer for '{}'", encodedPartition);
        }

        startOffsets.remove(encodedPartition);
    }

    private void setRetryTimeout(long timeoutMs) {
        context.timeout(timeoutMs);
    }

}
