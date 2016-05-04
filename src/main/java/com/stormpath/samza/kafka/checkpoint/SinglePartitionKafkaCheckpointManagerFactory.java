package com.stormpath.samza.kafka.checkpoint;

import kafka.consumer.ConsumerConfig;
import kafka.utils.ZKStringSerializer$;
import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.samza.checkpoint.CheckpointManager;
import org.apache.samza.checkpoint.CheckpointManagerFactory;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.KafkaConfig;
import org.apache.samza.config.KafkaProducerConfig;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.serializers.CheckpointSerde;
import org.apache.samza.util.ClientUtilTopicMetadataStore;
import org.apache.samza.util.ExponentialSleepStrategy;
import org.apache.samza.util.KafkaUtil;
import scala.Some;
import scala.collection.immutable.Map;
import scala.collection.immutable.Map$;

import java.util.Properties;
import java.util.UUID;

import static com.stormpath.samza.kafka.checkpoint.ScalaUtils.require;
import static com.stormpath.samza.kafka.checkpoint.ScalaUtils.val;

public class SinglePartitionKafkaCheckpointManagerFactory implements CheckpointManagerFactory {

    //Version number to track the format of the checkpoint log
    private static final int CHECKPOINT_LOG_VERSION_NUMBER = 1;

    private static final Map<String, String> INJECTED_PRODUCER_PROPERTIES =
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

    @Override
    public CheckpointManager getCheckpointManager(Config c, MetricsRegistry registry) {

        KafkaConfig config = new KafkaConfig(c);

        final String clientId = KafkaUtil.getClientId("samza-checkpoint-manager", config);
        final String systemName = require(config.getCheckpointSystem(), "no system defined for Kafka's checkpoint manager.");

        final KafkaProducerConfig producerConfig =
            config.getKafkaSystemProducerConfig(systemName, clientId, INJECTED_PRODUCER_PROPERTIES);
        final String consumerGroupId = "undefined-samza-consumer-group-" + UUID.randomUUID().toString();
        final ConsumerConfig consumerConfig =
            config.getKafkaSystemConsumerConfig(systemName, clientId, consumerGroupId, Map$.MODULE$.empty());

        final int replicationFactor = Integer.parseInt(config.getCheckpointReplicationFactor().getOrElse(val("3")));
        final int socketTimeout = consumerConfig.socketTimeoutMs();
        final int bufferSize = consumerConfig.socketReceiveBufferBytes();
        final int fetchSize = consumerConfig.fetchMessageMaxBytes(); // must be > buffer size

        final String zkConnect = require(new Some<>(consumerConfig.zkConnect()), "no zookeeper.connect defined in config");

        JobConfig jobConfig = new JobConfig(c);

        final String jobName = require(jobConfig.getName(), "Missing job name in configs");

        final String jobId = jobConfig.getJobId().getOrElse(val("1"));
        final String bootstrapServers = producerConfig.bootsrapServers();
        final ClientUtilTopicMetadataStore metadataStore =
            new ClientUtilTopicMetadataStore(bootstrapServers, clientId, socketTimeout);
        final String checkpointTopic = getTopic(jobName, jobId);

        // Find out the SSPGrouperFactory class so it can be included/verified in the key
        final String systemStreamPartitionGrouperFactoryString = jobConfig.getSystemStreamPartitionGrouperFactory();

        return new SinglePartitionKafkaCheckpointManager(
            clientId,
            checkpointTopic,
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

    private static String getTopic(String jobName, String jobId) {
        return String.format("__samza_checkpoint_ver_%d_for_%s_%s",
            CHECKPOINT_LOG_VERSION_NUMBER, jobName.replaceAll("_", "-"), jobId.replaceAll("_", "-"));
    }
}
