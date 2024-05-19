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
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

public class ConsumerDemo {
    public  static final Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);

    public static void main(String[] args) {
        logger.info( "I am a Kafka Consumer!" );

        Properties properties = new Properties();
        properties.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:19092,localhost:29092");
        properties.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(GROUP_ID_CONFIG, "lamaye");

        try (KafkaConsumer<String,String> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList("demo"));
            Duration timeout = Duration.ofMillis(100);
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(timeout);
                for (ConsumerRecord<String, String> consumerRecord : records) {
                    System.out.println(consumerRecord.key() + " " + consumerRecord.value());
                }

            }
        }



    }
}
