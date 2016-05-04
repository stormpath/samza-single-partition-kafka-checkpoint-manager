package com.stormpath.samza.kafka.checkpoint;

import org.apache.samza.SamzaException;

/**
 * KafkaCheckpointManager handles retries, so we need two kinds of exceptions:
 * one to signal a hard failure, and the other to retry. The
 * KafkaCheckpointException is thrown to indicate a hard failure that the Kafka
 * CheckpointManager can't recover from.
 */
public class KafkaCheckpointException extends SamzaException {

    public KafkaCheckpointException(String s) {
        super(s);
    }

    public KafkaCheckpointException(String s, Throwable t) {
        super(s, t);
    }
}
