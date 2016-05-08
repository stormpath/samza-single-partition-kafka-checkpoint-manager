package com.stormpath.samza.kafka.checkpoint;

import com.stormpath.samza.lang.Assert;
import com.stormpath.samza.lang.Collections;
import com.stormpath.samza.lang.Strings;
import kafka.consumer.ConsumerConfig;
import kafka.utils.ZKStringSerializer$;
import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.samza.checkpoint.CheckpointManager;
import org.apache.samza.checkpoint.CheckpointManagerFactory;
import org.apache.samza.config.*;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.serializers.CheckpointSerde;
import org.apache.samza.util.ClientUtilTopicMetadataStore;
import org.apache.samza.util.ExponentialSleepStrategy;
import org.apache.samza.util.KafkaUtil;
import scala.collection.immutable.Map$;

import java.util.*;

import static com.stormpath.samza.lang.ScalaUtils.require;
import static com.stormpath.samza.lang.ScalaUtils.val;

@SuppressWarnings("Duplicates")
public class PartitionedKafkaCheckpointManagerFactory implements CheckpointManagerFactory {

    //Version number to track the format of the checkpoint log
    private static final int CHECKPOINT_LOG_VERSION_NUMBER = 1;

    private static final scala.collection.immutable.Map<String, String> INJECTED_PRODUCER_PROPERTIES =
        new scala.collection.immutable.Map.Map2<>("acks", "all", "compression.type", "none");

    // Set the checkpoint topic configs to have a very small segment size and
    // enable log compaction. This keeps job startup time small since there
    // are fewer useless (overwritten) messages to read from the checkpoint
    // topic.
    private static Properties getCheckpointTopicProperties(KafkaConfig config) {
        String segmentBytes = config.getCheckpointSegmentBytes().getOrElse(val("26214400"));
        Properties props = new Properties();
        props.setProperty("cleanup.policy", "compact");
        props.setProperty("segment.bytes", segmentBytes);
        return props;
    }

    protected List<String> getTaskInputStreamNames(Config c) {
        final String key = TaskConfig$.MODULE$.INPUT_STREAMS();
        String val = Assert.hasText(c.get(key), "Missing " + key + " config value.");
        String[] inputs = Strings.commaDelimitedListToStringArray(val);
        List<String> list = Collections.listOf(inputs);
        return java.util.Collections.unmodifiableList(list);
    }

    private static String getTopic(String inputStreamName) {
        int version = CHECKPOINT_LOG_VERSION_NUMBER;
        return "__samza_partitioned_checkpoint_ver_" + version + "_for_" + inputStreamName.replaceAll("_", "-");
    }

    /**
     * Map key: task input stream name
     * Map value: the checkpoint topic name to use for checkpoints attributed to that input stream
     * @param c
     * @return
     */
    protected Map<String,String> getStreamCheckpointTopicNames(Config c) {

        List<String> inputStreamNames = getTaskInputStreamNames(c);
        Map<String,String> map = new LinkedHashMap<>(inputStreamNames.size());

        for(String inputStreamName : inputStreamNames) {
            String inputStreamCheckpointTopicName = getTopic(inputStreamName);
            map.put(inputStreamName, inputStreamCheckpointTopicName);
        }

        return java.util.Collections.unmodifiableMap(map);
    }

    @Override
    public CheckpointManager getCheckpointManager(Config c, MetricsRegistry registry) {

        KafkaConfig config = new KafkaConfig(c);

        final String clientId = KafkaUtil.getClientId("samza-checkpoint-manager", config);
        final java.util.Map<String,String> streamCheckpointTopicNames = getStreamCheckpointTopicNames(c);

        final String systemName = require(config.getCheckpointSystem(), "no system defined for Kafka checkpoint manager.");

        final KafkaProducerConfig producerConfig = config.getKafkaSystemProducerConfig(
            systemName, clientId, INJECTED_PRODUCER_PROPERTIES);

        final String consumerGroupId = "undefined-samza-consumer-group-" + UUID.randomUUID().toString();
        final ConsumerConfig consumerConfig = config.getKafkaSystemConsumerConfig(
            systemName, clientId, consumerGroupId, Map$.MODULE$.empty());

        final int replicationFactor = Integer.parseInt(config.getCheckpointReplicationFactor().getOrElse(val("3")));
        final int socketTimeout = consumerConfig.socketTimeoutMs();
        final int bufferSize = consumerConfig.socketReceiveBufferBytes();
        final int fetchSize = consumerConfig.fetchMessageMaxBytes(); // must be > buffer size

        final String zkConnect = Assert.hasText(consumerConfig.zkConnect(), "no zookeeper.connect defined in config.");

        JobConfig jobConfig = new JobConfig(c);
        final String jobName = require(jobConfig.getName(), "Missing job name in configs");

        final String bootstrapServers = producerConfig.bootsrapServers();
        final ClientUtilTopicMetadataStore metadataStore =
            new ClientUtilTopicMetadataStore(bootstrapServers, clientId, socketTimeout);

        final String checkpointTopicName = getTopic(jobName);

        // Find out the SSPGrouperFactory class so it can be included/verified in the key
        final String systemStreamPartitionGrouperFactoryString = jobConfig.getSystemStreamPartitionGrouperFactory();

        return new PartitionedKafkaCheckpointManager(
            clientId,
            checkpointTopicName,
            streamCheckpointTopicNames,
            systemName,
            replicationFactor,
            socketTimeout,
            bufferSize,
            fetchSize,
            metadataStore,
            () -> new KafkaProducer<>(producerConfig.getProducerProperties()),
            () -> new ZkClient(zkConnect, 6000, 6000, ZKStringSerializer$.MODULE$),
            systemStreamPartitionGrouperFactoryString,
            new ExponentialSleepStrategy(2.0, 100, 10000),
            new CheckpointSerde(),
            getCheckpointTopicProperties(config));
    }
}
