package com.siak.consumer;

import com.siak.interceptor.CustomerInterceptor;
import com.siak.model.customer.Customer;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.CommonClientConfigs.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

public class ConsumerAvroDemo {
    public  static final Logger logger = LoggerFactory.getLogger(ConsumerAvroDemo.class);

    public static void main(String[] args) {
        var config = new Properties();
        config.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:19092");
        config.put(KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        config.put(VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        config.put(GROUP_ID_CONFIG, "appAvro");

        config.put("schema.registry.url", "http://localhost:8081");

        try(KafkaConsumer<String, Customer> consumer = new KafkaConsumer<>(config)){
            consumer.subscribe(Collections.singletonList("demoAvro"));
            while (true) {
              ConsumerRecords<String, Customer> records = consumer.poll(Duration.ofMillis(100));
              for(ConsumerRecord<String, Customer> consumerRecord: records) {
                  logger.info(String.format("Key:%s Value:%s " +
                                  "Topic:%s Partition: %d Offset: %d",
                          consumerRecord.key(),
                          consumerRecord.value(),
                          consumerRecord.topic(),
                          consumerRecord.partition(),
                          consumerRecord.offset()));
              }
            }
        }
    }
}
