package com.siak.stream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.regex.Pattern;

import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG;

public class WordCountApplication {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(APPLICATION_ID_CONFIG, "word-count");
        props.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:19092");
        props.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String,String> source = builder.stream("wordcount-input");

        final Pattern pattern = Pattern.compile("\\W+");
        KStream<String, String> counts = source
                .flatMapValues(value -> Arrays.asList(pattern.split(value.toLowerCase())))
                .map(((key, value) -> new KeyValue<>(value, value)))
                .filter((key, value) -> (!value.equals("the")))
                .groupByKey()
                .count()
                .mapValues(value -> Long.toString(value)).toStream();

        counts.to("wordcount-output", Produced.with(Serdes.String(), Serdes.String()));

        Topology topology = builder.build();

        try(KafkaStreams streams = new KafkaStreams(topology, props)) {
            final CountDownLatch latch = new CountDownLatch(1);
            // attach shutdown handler to catch control-c
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                streams.close();
                latch.countDown();
            }));

            try {
                streams.start();
                latch.await();
            } catch (Exception e) {
                Thread.currentThread().interrupt();
                System.exit(1);
            }

            System.exit(0);
        }

    }
}
