package com.foo;

import com.foo.model.Event;
import com.foo.util.FooSerdes;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.UsePreviousTimeOnInvalidTimestamp;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.foo.EventDataConsumer.SINK_TOPIC_NAME;
import static com.foo.util.MockEventDataProducer.EVENTS_TOPIC;
import static org.apache.kafka.common.config.TopicConfig.MAX_COMPACTION_LAG_MS_CONFIG;
import static org.apache.kafka.streams.Topology.AutoOffsetReset.EARLIEST;

public class App {

    static Logger LOG = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) {
        /*
         * Increasing bufferIntervalInSeconds has adverse effect on
         *
         * <ul>
         *   <li>Length of transaction - i.e. if you were to change bufferFlushIntervalInMillis from 10 ms
         *       to 1000 ms, events buffered for 1000 ms > Events buffered for 10 ms. Thus time taken to
         *       complete one transaction will increase. Hence {@link
         *       org.apache.kafka.clients.producer.ProducerConfig#TRANSACTION_TIMEOUT_CONFIG} should be
         *       increased accordingly. Note that this should NOT be more than transaction.max.timeout.ms at broker config
         *   <li>Poll interval event-sequencer-app while consuming events from upstream. This is directly
         *       related to how long it takes to process the topology depth first, which in-turn depends
         *       on the number of events processed from buffer, which in turn depends on the buffer
         *       interval.
         *       <p>If you have configured poll interval as 10 ms. i.e. you are telling kafka broker to
         *       consider EventSequencer as dead if it didn't poll in 10ms and it takes 40 ms to process X
         *       events buffered in 1000 ms, kafka broker cuts the Event Sequencer from the consumer group
         *       thinking it is dead. Hence increase {@link
         *       org.apache.kafka.clients.consumer.ConsumerConfig#MAX_POLL_INTERVAL_MS_CONFIG}
         *       accordingly.
         * </ul>
         */
        long bufferIntervalInSeconds = 2;
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "sort-app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, "exactly_once");
        //  committing once in 3 seconds > buffer interval.
        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, (bufferIntervalInSeconds + 1) * 1000);
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, FooSerdes.EventSerde.class);
        config.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        config.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 60 * 1000);
        config.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 60 * 1000);

        Serde<String> stringSerde = Serdes.String();
        FooSerdes.EventSerde eventSerde = new FooSerdes.EventSerde();

        Topology toplogy = new Topology();
        String stateStoreName = "sort-state-store";

        KeyValueBytesStoreSupplier storeSupplier = Stores.inMemoryKeyValueStore(stateStoreName);
        // compact more often since you will be clearing state store quickly
        Map<String, String> logConfig = Map.of(MAX_COMPACTION_LAG_MS_CONFIG, "100");

        StoreBuilder<KeyValueStore<String, Event>> storeBuilder = Stores.keyValueStoreBuilder(storeSupplier, stringSerde, eventSerde)
                .withLoggingEnabled(logConfig);

        toplogy.addSource(EARLIEST,
                "source-sort-processor",
                new UsePreviousTimeOnInvalidTimestamp(),
                stringSerde.deserializer(),
                eventSerde.deserializer(),
                EVENTS_TOPIC)
                .addProcessor(SortProcessor.getName(),
                        () -> new SortProcessor(stateStoreName, bufferIntervalInSeconds),
                        "source-sort-processor")
                .addStateStore(storeBuilder, SortProcessor.getName())
                .addSink("sink-sort-processor",
                        SINK_TOPIC_NAME, stringSerde.serializer(), eventSerde.serializer(), SortProcessor.getName());

        KafkaStreams streams = new KafkaStreams(toplogy, config);

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit( () -> {
            System.out.println("Starting Application now");
            streams.start();
        });

        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            System.out.println("Shutting down Application  now");
            streams.close(Duration.ofSeconds(2));
        }));
    }
}
