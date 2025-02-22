package com.siak.consumer;

import com.siak.handler.HandleRebalanced;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;

import static java.util.Collections.singletonList;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

public class ConsumerRebalancedListenersDemo {
    public  static final Logger logger = LoggerFactory.getLogger(ConsumerRebalancedListenersDemo.class);

    @SuppressWarnings("InfiniteLoopStatement")
    public static void main(String[] args) {
        logger.info( "I am a Kafka Consumer Rebalanced listeners !" );

        Properties properties = new Properties();
        properties.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:39092");
        properties.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.put(ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(GROUP_ID_CONFIG, "Rebalanced");

        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(properties);
        HandleRebalanced handleRebalanced = new HandleRebalanced(consumer);

        final Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");
            consumer.wakeup();
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                logger.error(e.getMessage());
                Thread.currentThread().interrupt();
            }
        }));

        try {
            consumer.subscribe(singletonList("RebalancedTopic"), new HandleRebalanced(consumer));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> consumerRecord : records) {
                    logger.info("Partition: {}, Key: {}, Value: {}, offset: {} ", consumerRecord.key(),
                            consumerRecord.value(), consumerRecord.partition(), consumerRecord.offset());

                    handleRebalanced.addOffsetToTrack(consumerRecord.topic(),
                            consumerRecord.partition(), consumerRecord.offset());
                }
                consumer.commitAsync();
            }
        } catch (WakeupException e) {
            // we ignore this as this is an expected exception when closing a consumer
        } catch (Exception e) {
            logger.error("Unexpected exception", e);
        } finally {
            try {
                consumer.commitSync(handleRebalanced.getCurrentOffsets());
            } finally {
                consumer.close();
                logger.info("The consumer is now gracefully closed.");
            }
        }
    }
}

