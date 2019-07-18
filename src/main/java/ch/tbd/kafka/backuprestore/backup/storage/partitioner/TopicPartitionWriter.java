package ch.tbd.kafka.backuprestore.backup.storage.partitioner;

import ch.tbd.kafka.backuprestore.backup.kafkaconnect.config.BackupSinkConnectorConfig;
import ch.tbd.kafka.backuprestore.backup.storage.format.KafkaRecordWriterMultipartUpload;
import ch.tbd.kafka.backuprestore.backup.storage.format.RecordWriter;
import ch.tbd.kafka.backuprestore.common.kafkaconnect.AbstractBaseConnectorConfig;
import ch.tbd.kafka.backuprestore.util.AmazonS3Utils;
import com.amazonaws.services.s3.AmazonS3;
import io.confluent.connect.storage.partitioner.Partitioner;
import io.confluent.connect.storage.partitioner.PartitionerConfig;
import io.confluent.connect.storage.partitioner.TimeBasedPartitioner;
import io.confluent.connect.storage.partitioner.TimestampExtractor;
import io.confluent.connect.storage.util.DateTimeUtils;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.IllegalWorkerStateException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.errors.SchemaProjectorException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.joda.time.DateTime;
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
    private final io.confluent.connect.storage.partitioner.Partitioner<?> partitioner;
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
    private final BackupSinkConnectorConfig connectorConfig;
    private AmazonS3 amazonS3;
    private final long rotateScheduleIntervalMs;
    private DateTimeZone timeZone;
    private long nextScheduledRotation;
    private final TimestampExtractor timestampExtractor;

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

        this.timestampExtractor = partitioner instanceof TimeBasedPartitioner
                ? ((TimeBasedPartitioner) partitioner).getTimestampExtractor()
                : null;

        this.flushSize = this.connectorConfig.getFlushSize();
        rotateIntervalMs = this.connectorConfig.getRotateIntervalMs();

        if (rotateIntervalMs > 0 && timestampExtractor == null) {
            log.warn(
                    "Property '{}' is set to '{}ms' but partitioner is not an instance of '{}'. This property"
                            + " is ignored.",
                    AbstractBaseConnectorConfig.ROTATE_INTERVAL_MS_CONFIG,
                    rotateIntervalMs,
                    TimeBasedPartitioner.class.getName()
            );
        }

        rotateScheduleIntervalMs =
                connectorConfig.getLong(AbstractBaseConnectorConfig.ROTATE_SCHEDULE_INTERVAL_MS_CONFIG);
        if (rotateScheduleIntervalMs > 0) {
            try {
                timeZone = DateTimeZone.forID(connectorConfig.getString(PartitionerConfig.TIMEZONE_CONFIG));
            } catch (ConfigException e) {
                log.warn("No timezone defined. Will configure the default UTC");
                timeZone = DateTimeZone.forID("UTC");
            }
        }

        timeoutMs = this.connectorConfig.getRetryBackoffDefault();
        this.amazonS3 = AmazonS3Utils.initConnection(this.connectorConfig);

        // Initialize scheduled rotation timer if applicable
        setNextScheduledRotation();
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
                        encodedPartition, now
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
            String encodedPartition, long now
    ) {
        if (rotateOnTime(encodedPartition, currentTimestamp, now)) {
            setNextScheduledRotation();
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
                setNextScheduledRotation();

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

    private void setNextScheduledRotation() {
        if (rotateScheduleIntervalMs > 0) {
            long now = time.milliseconds();
            nextScheduledRotation = DateTimeUtils.getNextTimeAdjustedByDay(
                    now,
                    rotateScheduleIntervalMs,
                    timeZone
            );
            if (log.isDebugEnabled()) {
                log.debug(
                        "Update scheduled rotation timer. Next rotation for {} will be at {}",
                        tp,
                        new DateTime(nextScheduledRotation).withZone(timeZone).toString()
                );
            }
        }
    }

    public void close() {
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
                && timestampExtractor != null
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

        boolean scheduledRotation = rotateScheduleIntervalMs > 0 && now >= nextScheduledRotation;
        log.trace(
                "Should apply scheduled rotation: (rotateScheduleIntervalMs: '{}', nextScheduledRotation:"
                        + " '{}', now: '{}')? {}",
                rotateScheduleIntervalMs,
                nextScheduledRotation,
                now,
                scheduledRotation
        );
        return periodicRotation || scheduledRotation;
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

    private RecordWriter getWriter(String encodedPartition) {
        if (writers.containsKey(encodedPartition)) {
            return writers.get(encodedPartition);
        }
        // TODO: Optimize how to extract an instance of RecordWriter
        RecordWriter writer = new KafkaRecordWriterMultipartUpload(connectorConfig, amazonS3);
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

        RecordWriter writer = getWriter(currentEncodedPartition);
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
            log.warn("Tried to commit file with missing starting offset partition: {}. Ignoring.", encodedPartition);
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
