package com.siak.consumer;

import com.siak.handler.HandleRebalanced;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

public class ConsumerRebalancedListenersDemo {
    public  static final Logger logger = LoggerFactory.getLogger(ConsumerRebalancedListenersDemo.class);

    public static void main(String[] args) {
        logger.info( "I am a Kafka Consumer Rebalanced listeners !" );

        Properties properties = new Properties();
        properties.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:39092");
        properties.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.put(ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(GROUP_ID_CONFIG, "Rebalanced");

        Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
        try( KafkaConsumer<String,String> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList("RebalancedTopic"),
                    new HandleRebalanced(consumer, currentOffsets));

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> consumerRecord : records) {
                    System.out.printf("partition = %d,  offset = %d, key = %s, value = %s%n", consumerRecord.partition(),
                            consumerRecord.offset(), consumerRecord.key(), consumerRecord.value());
                    currentOffsets.put(
                            new TopicPartition(consumerRecord.topic(), consumerRecord.partition()),
                            new OffsetAndMetadata(consumerRecord.offset()+1, null));
                }
                consumer.commitAsync(currentOffsets, null);
            }
        }
    }
}

