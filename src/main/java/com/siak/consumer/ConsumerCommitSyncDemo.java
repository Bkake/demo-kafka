package com.siak.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

public class ConsumerCommitSyncDemo {
    public  static final Logger logger = LoggerFactory.getLogger(ConsumerCommitSyncDemo.class);

    public static void main(String[] args) {
        logger.info( "I am a Kafka Consumer CommitSync!" );

        Properties properties = new Properties();
        properties.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:19092,localhost:29092");
        properties.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.put(GROUP_ID_CONFIG, "sona");
        properties.put(ENABLE_AUTO_COMMIT_CONFIG, false);

        try (KafkaConsumer<String,String> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList("demoCommitSync"));
            Duration timeout = Duration.ofMillis(100);
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(timeout);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("partition = %d,  offset = %d, key = %s, value = %s%n", record.partition(),
                            record.offset(), record.key(), record.value());
                }
                consumer.commitSync();
            }
        }



    }
}
