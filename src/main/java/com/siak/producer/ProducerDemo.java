package com.siak.producer;


import com.siak.config.KafkaGenericConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

public class ProducerDemo {
    public  static final Logger logger = LoggerFactory.getLogger(ProducerDemo.class);
    public static void main( String[] args ) {
        logger.info( "I am a Kafka Producer!" );

        var bootStrapServers = "localhost:19092,localhost:39092,localhost:39092";
        var topicName = "demo";

        // create Producer properties
        var properties = KafkaGenericConfig.defaultProducerConfig(bootStrapServers,
                30000, 3, new HashMap<>());
        properties.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String,String> producer = new KafkaProducer<>(properties,
                new StringSerializer(), new StringSerializer());

        for (int i = 1; i <= 2 ; i++) {
            var key = "id_"+i;
            var message = "HelloWorld" + i;

            // create a producer record
            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<>(topicName, key, message);

            // send data - async
            producer.send(producerRecord, (recordMetadata, e) -> {
                if (e == null) {
                    logger.info(String.format("Received new metadata " +
                                    "Topic:%s Partition: %d Offset: %d Timestamp: %d",
                            recordMetadata.topic(),
                            recordMetadata.partition(),
                            recordMetadata.offset(),
                            recordMetadata.timestamp()));
                } else {
                    logger.error("Error while producing", e);
                }
            });
        }

        // flush data - sync
        producer.flush();

        // flush and close producer
        producer.close();

    }
}
