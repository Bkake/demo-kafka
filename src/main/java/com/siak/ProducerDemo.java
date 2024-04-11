package com.siak;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

public class ProducerDemo {
    public  static final Logger logger = LoggerFactory.getLogger(ProducerDemo.class);
    public static void main( String[] args ) {
        logger.info( "I am a Kafka Producer!" );

        var bootStrapServers = "localhost:19092";

        // create Producer properties
        var properties = new Properties();
        properties.put(BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        properties.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);

        // create a producer record
        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<>("demo", "Message1");


        // send data - async
        producer.send(producerRecord);

        // flush data - sync
        producer.flush();

        // flush and close producer
        producer.close();

    }
}
