package com.siak.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

public class ConsumerWekeupDemo {
    public  static final Logger logger = LoggerFactory.getLogger(ConsumerWekeupDemo.class);

    public static void main(String[] args) {
        logger.info( "I am a Kafka Consumer Wakeup!" );

        Properties properties = new Properties();
        properties.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        properties.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.put(GROUP_ID_CONFIG, "lamaye-wakeup");

        String topic = "wakeupTopic";

        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(properties);

        final Thread mainThread = Thread.currentThread();

        // adding the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");
            consumer.wakeup();

            try {
                mainThread.join();
            } catch (InterruptedException e) {
                logger.error(e.getMessage());
            }
        }));

        try {
            consumer.subscribe(Collections.singletonList(topic));
            while (true) {
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    logger.info("Key: " + record.key() + ", Value: " + record.value());
                    logger.info("Partition: " + record.partition() + ", Offset:" + record.offset());
                }
            }
        } catch (WakeupException e) {
            logger.info("Wakeup exception");
        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            consumer.close();
            logger.info("The consumer is now gracefully closed.");
        }

    }
}
