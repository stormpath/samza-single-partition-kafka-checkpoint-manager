package com.stormpath.samza.zookeeper.checkpoint;

import com.stormpath.samza.lang.Strings;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.samza.SamzaException;
import org.apache.samza.checkpoint.CheckpointManager;
import org.apache.samza.checkpoint.CheckpointManagerFactory;
import org.apache.samza.config.Config;
import org.apache.samza.metrics.MetricsRegistry;

import java.util.function.Supplier;

@SuppressWarnings("Duplicates")
public class ZookeeperCheckpointManagerFactory implements CheckpointManagerFactory {

    private String getRequiredString(Config config, String key) {
        String val = Strings.clean(config.get(key));
        if (val == null) {
            String msg = "Configuration value missing for required property '" + key + "'";
            throw new SamzaException(msg);
        }
        return val;
    }

    @Override
    public CheckpointManager getCheckpointManager(Config config, MetricsRegistry registry) {

        //RetryPolicy
        int baseSleepTimeMs = config.getInt("task.checkpoint.zookeeper.client.retryPolicy.baseSleepTimeMs", 1000);
        int maxRetries = config.getInt("task.checkpoint.zookeeper.client.retryPolicy.maxRetries", 29);
        int maxSleepMs = config.getInt("task.checkpoint.zookeeper.client.retryPolicy.maxSleepMs", Integer.MAX_VALUE);
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(baseSleepTimeMs, maxRetries, maxSleepMs);

        //Client
        String jobName = getRequiredString(config, "job.name");
        String basePath = config.get("task.checkpoint.zookeeper.client.namespace", "/samza/jobs");
        String jobPath = basePath + "/" + jobName;
        String connectString = getRequiredString(config, "task.checkpoint.zookeeper.client.connectString");
        int sessionTimeoutMs = config.getInt("task.checkpoint.zookeeper.client.sessionTimeoutMs", 60_000);
        int connectionTimeoutMs = config.getInt("task.checkpoint.zookeeper.client.connectionTimeoutMs", 15_000);

        Supplier<CuratorFramework> curatorFactory = () ->
            CuratorFrameworkFactory.builder()
                .defaultData(new byte[0]) //dunno why, but curator defaults to the host IP - not useful this use case
                .connectString(connectString)
                .connectionTimeoutMs(connectionTimeoutMs)
                .sessionTimeoutMs(sessionTimeoutMs)
                .retryPolicy(retryPolicy)
                .build();

        return new ZookeeperCheckpointManager(curatorFactory, jobPath);
    }
}
