package com.siak.handler;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

public class HandleRebalanced implements ConsumerRebalanceListener {
    private  static final Logger logger =
            LoggerFactory.getLogger(HandleRebalanced.class);

    public KafkaConsumer<String,String> consumer;
    public Map<TopicPartition, OffsetAndMetadata> currentOffsets;
    public HandleRebalanced(KafkaConsumer<String,String> consumer,
                            Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        this.consumer = consumer;
        this.currentOffsets = currentOffsets;
    }
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        logger.info("Lost partitions in rebalanced. " +
                "Committing current offsets: {}", currentOffsets);
        consumer.commitSync(currentOffsets);
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        partitions.forEach(topicPartition -> logger.info("topic {}, partition {}",
                topicPartition.topic(), topicPartition.partition()));

    }
}
