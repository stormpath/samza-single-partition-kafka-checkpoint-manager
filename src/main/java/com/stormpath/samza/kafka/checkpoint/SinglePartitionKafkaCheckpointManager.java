package com.stormpath.samza.kafka.checkpoint;

import kafka.admin.AdminUtils$;
import kafka.api.*;
import kafka.cluster.Broker;
import kafka.common.ErrorMapping$;
import kafka.common.InvalidMessageSizeException;
import kafka.common.TopicAndPartition;
import kafka.common.TopicExistsException;
import kafka.consumer.SimpleConsumer;
import kafka.message.ByteBufferMessageSet;
import kafka.message.InvalidMessageException;
import kafka.message.MessageAndOffset;
import kafka.utils.Utils;
import kafka.utils.Utils$;
import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.samza.SamzaException;
import org.apache.samza.checkpoint.Checkpoint;
import org.apache.samza.checkpoint.CheckpointManager;
import org.apache.samza.checkpoint.kafka.KafkaCheckpointLogKey;
import org.apache.samza.checkpoint.kafka.KafkaCheckpointLogKey$;
import org.apache.samza.container.TaskName;
import org.apache.samza.serializers.CheckpointSerde;
import org.apache.samza.system.kafka.TopicMetadataCache$;
import org.apache.samza.util.ExponentialSleepStrategy;
import org.apache.samza.util.KafkaUtil$;
import org.apache.samza.util.TopicMetadataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.collection.JavaConversions;

import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static com.stormpath.samza.kafka.checkpoint.ScalaUtils.*;

public class SinglePartitionKafkaCheckpointManager implements CheckpointManager {

    static final Logger log = LoggerFactory.getLogger(SinglePartitionKafkaCheckpointManager.class);

    public static final String CHECKPOINT_LOG4J_ENTRY = "checkpoint log";
    public static final String CHANGELOG_PARTITION_MAPPING_LOG4j = "changelog partition mapping";

    private final String clientId;
    private final String checkpointTopic;
    private final String systemName;
    private final int replicationFactor;
    private final int socketTimeout;
    private final int bufferSize;
    private final int fetchSize;
    private final TopicMetadataStore topicMetadataStore;
    private final Supplier<Producer<byte[], byte[]>> connectProducer;
    private final Supplier<ZkClient> connectZk;
    private final ExponentialSleepStrategy retryBackoff;
    private final CheckpointSerde serde;
    private final Properties checkpointTopicProperties;
    private final Set<TaskName> taskNames = new HashSet<>();

    private Producer<byte[], byte[]> producer;
    private Map<TaskName, Checkpoint> taskNamesToOffsets = null;
    // Where to start reading for each subsequent call of readCheckpoint:
    private Option<Long> startingOffset = scala.Option.apply(null);

    public SinglePartitionKafkaCheckpointManager(String clientId, String checkpointTopic, String systemName,
                                                 int replicationFactor, int socketTimeout, int bufferSize,
                                                 int fetchSize, TopicMetadataStore topicMetadataStore,
                                                 Supplier<Producer<byte[], byte[]>> connectProducer,
                                                 Supplier<ZkClient> connectZk,
                                                 String systemStreamPartitionGrouperFactoryString,
                                                 ExponentialSleepStrategy retryBackoff,
                                                 CheckpointSerde serde,
                                                 Properties checkpointTopicProperties) {
        this.clientId = clientId;
        this.checkpointTopic = checkpointTopic;
        this.systemName = systemName;
        this.replicationFactor = replicationFactor;
        this.socketTimeout = socketTimeout;
        this.bufferSize = bufferSize;
        this.fetchSize = fetchSize;
        this.topicMetadataStore = topicMetadataStore;
        this.connectProducer = connectProducer;
        this.connectZk = connectZk;
        this.retryBackoff = retryBackoff;
        this.serde = serde;
        this.checkpointTopicProperties = checkpointTopicProperties;

        KafkaCheckpointLogKey$.MODULE$
            .setSystemStreamPartitionGrouperFactoryString(systemStreamPartitionGrouperFactoryString);

        log.info("Creating SinglePartitionKafkaCheckpointManager with: clientId={}, checkpointTopic={}, systemName={}",
            new Object[]{this.clientId, this.checkpointTopic, this.systemName});
    }

    @Override
    public void writeCheckpoint(TaskName taskName, Checkpoint checkpoint) {
        final KafkaCheckpointLogKey key = KafkaCheckpointLogKey$.MODULE$.getCheckpointKey(taskName);
        final byte[] keyBytes = key.toBytes();
        final byte[] msgBytes = serde.toBytes(checkpoint);
        writeLog(CHECKPOINT_LOG4J_ENTRY, keyBytes, msgBytes);
    }

    @Override
    public void writeChangeLogPartitionMapping(Map<TaskName, Integer> mapping) {
        final KafkaCheckpointLogKey key = KafkaCheckpointLogKey$.MODULE$.getChangelogPartitionMappingKey();
        final byte[] keyBytes = key.toBytes();
        final byte[] msgBytes = serde.changelogPartitionMappingToBytes(mapping);
        writeLog(CHANGELOG_PARTITION_MAPPING_LOG4j, keyBytes, msgBytes);
    }

    /**
     * Common code for writing either checkpoints or changelog-partition-mappings to the log
     *
     * @param logType Type of entry that is being written, for logging
     * @param key     pre-serialized key for message
     * @param msg     pre-serialized message to write to log
     */
    private void writeLog(final String logType, final byte[] key, final byte[] msg) {
        retry(
            loop -> {
                if (producer == null) {
                    producer = connectProducer.get();
                }
                try {
                    producer.send(new ProducerRecord<>(checkpointTopic, 0, key, msg)).get();
                } catch (Throwable t) {
                    throw new LoopException(t);
                }
                loop.done();
            },
            (ex, loop) -> {
                log.warn("Failed to write {} partition entry {}: {}. Retrying.", new Object[]{logType, key, ex.getMessage()});
                log.debug("Exception detail: ", ex);

                if (ex instanceof LoopException) {
                    Throwable cause = ex.getCause();
                    if (cause instanceof InterruptedException || cause instanceof ClosedByInterruptException) {
                        //not recoverable, propagate:
                        throw (LoopException) ex;
                    }
                    //otherwise we can retry, so cleanup:
                    if (producer != null) {
                        producer.close();
                    }
                    producer = null;
                }
            }
        );
    }

    private scala.collection.immutable.Map<String, TopicMetadata> getTopicMetadataMap() {
        return TopicMetadataCache$.MODULE$.getTopicMetadata(
            new scala.collection.immutable.Set.Set1<>(checkpointTopic),
            systemName,
            funf(topicMetadataStore::getTopicInfo),
            5000L,
            fun(System::currentTimeMillis));
    }

    private TopicMetadata getTopicMetadata() {

        scala.collection.immutable.Map<String, TopicMetadata> metadataMap = getTopicMetadataMap();

        TopicMetadata metadata = require(metadataMap.get(checkpointTopic),
            "No TopicMetadata for checkpointTopic " + checkpointTopic);

        KafkaUtil$.MODULE$.maybeThrowException(metadata.errorCode());

        int len = metadata.partitionsMetadata().length();

        if (len != 1) {
            String msg = String.format("Checkpoint topic validation failed for topic %s because partition " +
                "count %s did not match expected partition count of 1.", checkpointTopic, len);
            throw new KafkaCheckpointException(msg);
        }

        return metadata;
    }

    private PartitionMetadata getPartitionMetadata() {
        TopicMetadata metadata = getTopicMetadata();
        return JavaConversions.seqAsJavaList(metadata.partitionsMetadata())
            .stream()
            .filter(pm -> pm.partitionId() == 0)
            .findFirst()
            .orElseThrow(() -> new KafkaCheckpointException("Tried to find partition information for partition 0 " +
                "for checkpoint topic, but it didn't exist in Kafka."));
    }

    private SimpleConsumer getConsumer() {

        PartitionMetadata partitionMetadata = getPartitionMetadata();

        Broker leader = require(partitionMetadata.leader(), "No leader available for topic " + checkpointTopic);

        log.info("Connecting to leader {}:{} for topic {} and to fetch all checkpoint messages.",
            new Object[]{leader.host(), leader.port(), checkpointTopic});

        return new SimpleConsumer(leader.host(), leader.port(), socketTimeout, bufferSize, clientId);
    }

    private long getEarliestOffset(SimpleConsumer consumer, TopicAndPartition topicAndPartition) {
        return consumer.earliestOrLatestOffset(topicAndPartition, OffsetRequest$.MODULE$.EarliestTime(), -1);
    }

    private long getOffset(SimpleConsumer consumer, TopicAndPartition topicAndPartition, long earliestOrLatest) {

        scala.collection.immutable.Map.Map1<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo =
            new scala.collection.immutable.Map.Map1<>(topicAndPartition, new PartitionOffsetRequestInfo(earliestOrLatest, 1));

        OffsetRequest offsetRequest = new OffsetRequest(requestInfo, OffsetRequest$.MODULE$.CurrentVersion(), 0,
            OffsetRequest$.MODULE$.DefaultClientId(), Request$.MODULE$.OrdinaryConsumerId());

        PartitionOffsetsResponse response = require(
            consumer.getOffsetsBefore(offsetRequest).partitionErrorAndOffsets().get(topicAndPartition),
            "Unable to find offset information for " + checkpointTopic + ":0");

        // Fail or retry if there was an an issue with the offset request.
        KafkaUtil$.MODULE$.maybeThrowException(response.error());

        return (Long) require(response.offsets().headOption(),
            "Got response, but no offsets defined for " + checkpointTopic + ":0");
    }

    @Override
    public Checkpoint readLastCheckpoint(TaskName taskName) {

        if (!taskNames.contains(taskName)) {
            throw new SamzaException(taskName + " not registered with this CheckpointManager");
        }

        log.info("Reading checkpoint for taskName {}", taskName);

        if (taskNamesToOffsets == null) {
            log.info("No TaskName to checkpoint mapping provided.  Reading for first time.");
            taskNamesToOffsets = readCheckpointsFromLog();
        } else {
            log.info("Already existing checkpoint mapping.  Merging new offsets");
            taskNamesToOffsets.putAll(readCheckpointsFromLog());
        }

        Checkpoint checkpoint = taskNamesToOffsets.getOrDefault(taskName, null);

        log.info("Got checkpoint state for taskName {}: {}", taskName, checkpoint);

        return checkpoint;
    }

    /**
     * Read through entire log, discarding changelog mapping, and building map of TaskNames to Checkpoints
     */
    private Map<TaskName, Checkpoint> readCheckpointsFromLog() {

        final Map<TaskName, Checkpoint> checkpoints = new HashMap<>();

        //noinspection Convert2MethodRef
        readLog(CHECKPOINT_LOG4J_ENTRY,
            key -> key.isCheckpointKey(),
            (payload, checkpointKey) -> {
                TaskName taskName = checkpointKey.getCheckpointTaskName();
                if (taskNames.contains(taskName)) {
                    Checkpoint checkpoint = serde.fromBytes(Utils.readBytes(payload));
                    log.debug("Adding checkpoint {} for taskName {}", checkpoint, taskName);
                    checkpoints.put(taskName, checkpoint); //replacing any existing, older checkpoints as we go
                }
            });

        return checkpoints;
    }

    @Override
    public Map<TaskName, Integer> readChangeLogPartitionMapping() {

        final Map<TaskName, Integer> changelogPartitionMapping = new HashMap<>();

        //noinspection Convert2MethodRef
        readLog(CHANGELOG_PARTITION_MAPPING_LOG4j,
            key -> key.isChangelogPartitionMapping(),
            (payload, checkpointKey) -> {
                Map<TaskName, Integer> map = serde.changelogPartitionMappingFromBytes(Utils.readBytes(payload));
                changelogPartitionMapping.putAll(map);
                log.debug("Adding changelog partition mapping {}", map);
            });

        return changelogPartitionMapping;
    }

    /**
     * Common code for reading both changelog partition mapping and change log
     *
     * @param entryType   What type of entry to look for within the log key's
     * @param handleEntry Code to handle an entry in the log once it's found
     */
    private void readLog(String entryType,
                         Predicate<KafkaCheckpointLogKey> shouldHandleEntry,
                         BiConsumer<ByteBuffer, KafkaCheckpointLogKey> handleEntry) {

        retry(
            loop -> {
                final SimpleConsumer consumer = getConsumer();
                final TopicAndPartition topicAndPartition = new TopicAndPartition(checkpointTopic, 0);

                try {

                    long offset = startingOffset.getOrElse(fun(() -> getEarliestOffset(consumer, topicAndPartition)));

                    log.info("Got offset {} for topic {} and partition 0.  Attempting to fetch messages for {}",
                        new Object[]{offset, checkpointTopic, entryType});

                    long latestOffset = getOffset(consumer, topicAndPartition, OffsetRequest$.MODULE$.LatestTime());

                    log.info("Got latest offset {} for topic {} and partition 0.", latestOffset, checkpointTopic);

                    if (offset < 0) {
                        log.info("Got offset 0 (no messages in {}) for topic {} and partition 0, so returning " +
                            "empty collection. If you expected the checkpoint topic to have messages, you're " +
                            "probably going to lose data.", entryType, checkpointTopic);
                        return;
                    }


                    while (offset < latestOffset) {

                        final FetchRequest request = new FetchRequestBuilder()
                            .addFetch(checkpointTopic, 0, offset, fetchSize)
                            .maxWait(500)
                            .minBytes(1)
                            .clientId(clientId)
                            .build();

                        FetchResponse fetchResponse = consumer.fetch(request);
                        if (fetchResponse.hasError()) {
                            short errorCode = fetchResponse.errorCode(checkpointTopic, 0);
                            log.warn("Got error code from broker for {}: {}", checkpointTopic, errorCode);
                            if (ErrorMapping$.MODULE$.OffsetOutOfRangeCode() == errorCode) {
                                log.warn("Got an offset out of range exception while getting last entry in {} for " +
                                        "topic {} and partition 0, so returning a null offset to the KafkaConsumer. " +
                                        "Let it decide what to do based on its autooffset.reset setting.",
                                    entryType, checkpointTopic);
                                return;
                            }
                            KafkaUtil$.MODULE$.maybeThrowException(errorCode);
                        }

                        ByteBufferMessageSet set = fetchResponse.messageSet(checkpointTopic, 0);
                        for (MessageAndOffset response : JavaConversions.asJavaIterable(set)) {

                            offset = response.nextOffset();
                            startingOffset = scala.Option.apply(offset); // For next time we call

                            if (!response.message().hasKey()) {
                                throw new KafkaCheckpointException("Encountered message without key.");
                            }

                            KafkaCheckpointLogKey checkpointKey =
                                KafkaCheckpointLogKey$.MODULE$.fromBytes(
                                    Utils$.MODULE$.readBytes(response.message().key()));

                            if (!shouldHandleEntry.test(checkpointKey)) {
                                log.debug("Skipping {} entry with key {}", entryType, checkpointKey);
                            } else {
                                handleEntry.accept(response.message().payload(), checkpointKey);
                            }
                        }
                    }

                } finally {
                    consumer.close();
                }

                loop.done();
            },
            (ex, loop) -> {
                if (ex instanceof InvalidMessageException)
                    throw new KafkaCheckpointException("Got InvalidMessageException from Kafka, which is unrecoverable, so fail the samza job", ex);
                if (ex instanceof InvalidMessageSizeException)
                    throw new KafkaCheckpointException("Got InvalidMessageSizeException from Kafka, which is unrecoverable, so fail the samza job", ex);
                if (ex instanceof UnknownTopicOrPartitionException)
                    throw new KafkaCheckpointException("Got UnknownTopicOrPartitionException from Kafka, which is unrecoverable, so fail the samza job", ex);
                if (ex instanceof KafkaCheckpointException) throw (KafkaCheckpointException) ex;
                if (log.isWarnEnabled()) {
                    String msg = "Unable to read last " + entryType + " entry for topic " + checkpointTopic +
                        " and partition 0: " + ex.getMessage() + ".  Retrying.";
                    log.warn(msg);
                }
                log.debug("Exception details: ", ex);
            }
        );
    }

    @Override
    public void start() {
        create();
        validateTopic();
    }

    @Override
    public void register(TaskName taskName) {
        log.debug("Adding taskName {} to {}", taskName, this);
        taskNames.add(taskName);
    }

    @Override
    public void stop() {
        if (producer != null) {
            producer.close();
        }
    }

    private void create() {
        log.info("Attempting to create checkpoint topic {}.", checkpointTopic);
        retry(
            loop -> {
                ZkClient zkClient = connectZk.get();
                try {
                    AdminUtils$.MODULE$.createTopic(zkClient, checkpointTopic, 1, replicationFactor, checkpointTopicProperties);
                } finally {
                    zkClient.close();
                }
                log.info("Created checkpoint topic for {}.", checkpointTopic);
                loop.done();
            },
            (ex, loop) -> {
                if (ex instanceof TopicExistsException) {
                    log.info("Checkpoint topic " + checkpointTopic + " already exists.");
                    loop.done();
                } else {
                    String msg = "Failed to create topic " + checkpointTopic + ": " + ex.getMessage() + ".  Retrying.";
                    log.warn(msg, ex);
                }
            }
        );
    }

    private void validateTopic() {

        log.info("Validating checkpoint topic {}.", checkpointTopic);

        retry(
            loop -> {
                //the getPartitionMetadata() method has assertions that will throw exceptions if
                //expected conditions are not met.  Just calling that method is enough to validate the topic:
                getPartitionMetadata();
                log.info("Successfully validated checkpoint topic {}.", checkpointTopic);
                loop.done();
            },
            (ex, loop) -> {
                if (ex instanceof KafkaCheckpointException) {
                    throw (KafkaCheckpointException) ex; //can't retry, propagate
                }
                log.warn("Unable to validate topic " + checkpointTopic + ".  Retrying.", ex);
            }
        );
    }

    @Override
    public String toString() {
        return String.format("SinglePartitionKafkaCheckpointManager [systemName=%s, checkpointTopic=%s]",
            systemName, checkpointTopic);
    }

    private void retry(Consumer<ExponentialSleepStrategy.RetryLoop> c,
                       BiConsumer<Exception, ExponentialSleepStrategy.RetryLoop> bic) {
        retryBackoff.run(fun1(c), fun2(bic));
    }

    private static class LoopException extends RuntimeException {
        public LoopException(Throwable cause) {
            super(cause);
        }
    }
}
