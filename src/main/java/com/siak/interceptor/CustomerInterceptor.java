package com.siak.interceptor;

import com.siak.model.customer.Customer;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class CustomerInterceptor implements ProducerInterceptor<String, Customer> {
    public  static final Logger logger = LoggerFactory.getLogger(CustomerInterceptor.class);
    @Override
    public ProducerRecord<String, Customer> onSend(ProducerRecord<String, Customer> producerRecord) {
        if (producerRecord != null) {
            logger.info("Intercepted new record topic:{} key:{} value:{}",
                    producerRecord.topic(), producerRecord.key(), producerRecord.value());
            producerRecord.headers().add("privacy-level", "YOLO".getBytes(StandardCharsets.UTF_8));
        }

        return producerRecord;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if (exception == null) {
            logger.info(String.format("Received new metadata " +
                            "Topic:%s Partition: %d Offset: %d Timestamp: %d",
                    metadata.topic(),
                    metadata.partition(),
                    metadata.offset(),
                    metadata.timestamp()));
        } else {
            logger.error("Error while producing", exception);
        }
    }

    @Override
    public void close() {
        // Do nothing
    }

    @Override
    public void configure(Map<String, ?> configs) {
       // Do nothing
    }
}
