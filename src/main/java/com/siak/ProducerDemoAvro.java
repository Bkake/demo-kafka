package com.siak;


import com.siak.interceptor.CustomerInterceptor;
import com.siak.model.customer.Customer;
import com.siak.partitioner.CustomerPartitionner;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

import static com.siak.producer.KafkaGenericConfig.defaultProducerConfig;
import static org.apache.kafka.clients.producer.ProducerConfig.INTERCEPTOR_CLASSES_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.PARTITIONER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

public class ProducerDemoAvro {
    public  static final Logger logger = LoggerFactory.getLogger(ProducerDemoAvro.class);
    public static void main( String[] args )  {
        logger.info( "I am a Kafka Avro Producer!" );

        var bootStrapServers = "localhost:29092";
        var schemaUrl = "http://localhost:8081";

        var topicName = "demoAvro";

        var config = defaultProducerConfig(bootStrapServers,
                30000, 3, new HashMap<>());
        config.put(KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        config.put(VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        config.put(PARTITIONER_CLASS_CONFIG, CustomerPartitionner.class.getName());
        config.put(INTERCEPTOR_CLASSES_CONFIG, CustomerInterceptor.class.getName());
        config.put("schema.registry.url", schemaUrl);

        KafkaProducer<String, Customer> producer = new KafkaProducer<>(config);

        for (int i = 0; i < 10 ; i++) {
            var key = "id_"+ i;
            var name = "customer" + i;
            var email = "customer" + i + "@aol.com";

            Customer customer = Customer.newBuilder()
                    .setId(i).setName(name).setEmail(email).build();

            ProducerRecord<String, Customer> producerRecord =
                    new ProducerRecord<>(topicName, key, customer);

            // send data - async
            producer.send(producerRecord);
        }

        // flush data - sync
        producer.flush();

        // flush and close producer
        producer.close();
    }
}
