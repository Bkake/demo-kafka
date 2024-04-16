package com.siak.config;

import org.apache.commons.collections4.MapUtils;

import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.RETRIES_CONFIG;

public class KafkaGenericConfig {

    public static Properties defaultProducerConfig(String bootStrapServers,
                                                   Integer requestTimeout,
                                                   Integer maxRetries,
                                                   Map<String, Objects> additionalConfig) {
        var config = new Properties();
        config.put(BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        config.put(REQUEST_TIMEOUT_MS_CONFIG, requestTimeout);
        config.put(RETRIES_CONFIG, maxRetries);

        if (MapUtils.isNotEmpty(additionalConfig)) {
            config.putAll(additionalConfig);
        }
        return config;
    }
    private KafkaGenericConfig() {
        // Nothing to do
    }

}
