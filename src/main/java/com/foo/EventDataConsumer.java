package com.foo;

import com.foo.model.Event;
import com.foo.util.FooSerdes;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Printed;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

// This consumer runs with Read committed isolation - so the result you see from a console consumer command would be different from this. Don't get confused
public class EventDataConsumer {
    public static final String SINK_TOPIC_NAME = "events-upper";

    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "event-consumer-app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, "exactly_once");
        config.put(StreamsConfig.POLL_MS_CONFIG, 1000L);
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, FooSerdes.EventSerde.class);
        config.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);

        Serde<String> stringSerde = Serdes.String();
        FooSerdes.EventSerde eventSerde = new FooSerdes.EventSerde();
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        String changeLogTopicName = "sort-app-sort-state-store-changelog";

        streamsBuilder.stream(SINK_TOPIC_NAME, Consumed.with(stringSerde, eventSerde))
                .print(Printed.<String, Event>toSysOut().withLabel("output"));

        KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), config);
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(() -> {
            System.out.println("Starting Consumer now");
            streams.start();
        });

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down Consumer  now");
            streams.close(Duration.ofSeconds(2));
        }));
    }
}
