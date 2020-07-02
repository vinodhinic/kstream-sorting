package com.foo.util;

import com.foo.model.Event;
import com.foo.model.EventType;
import com.github.javafaker.Faker;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MockEventDataProducer {

    private static final Logger LOG = LoggerFactory.getLogger(MockEventDataProducer.class);

    private static Producer<String, String> producer;
    private static final Gson gson = new GsonBuilder().disableHtmlEscaping().create();
    private static ExecutorService executorService = Executors.newFixedThreadPool(1);
    private static Callback callback;
    public static final String EVENTS_TOPIC = "events";
    private static volatile boolean keepRunning = true;

    private static void init() {
        if (producer == null) {
            LOG.info("Initializing the producer");
            Properties properties = new Properties();
            properties.put("bootstrap.servers", "localhost:9092");
            properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            properties.put("acks", "1");
            properties.put("retries", "3");

            producer = new KafkaProducer<>(properties);

            callback = (metadata, exception) -> {
                if (exception != null) {
                    exception.printStackTrace();
                }
            };
            LOG.info("Producer initialized");
        }
    }

    private static <T> String convertToJson(T generatedDataItem) {
        return gson.toJson(generatedDataItem);
    }

    public static void produceEventData(int numberOfIterations, Duration delayBetweenEvents) throws ExecutionException, InterruptedException {
        Runnable generateTask = () -> {
            init();
            int counter = 0;
            while(counter < numberOfIterations && keepRunning) {
                Faker faker = new Faker();
                List<EventType> types = Stream.of(EventType.values()).collect(Collectors.toList());
                Collections.reverse(types);
                for (EventType eventType : types) {
                    if(eventType.equals(EventType.START) || eventType.equals(EventType.STOP)) {
                        continue;
                    }
                    Event e = Event.builder().eventType(eventType).payload(faker.chuckNorris().fact()).build();
                    ProducerRecord<String, String> record = new ProducerRecord<>(EVENTS_TOPIC,
                            e.getEventType().name(),
                            convertToJson(e));
                    producer.send(record, callback);
                    LOG.info("Sent {}", eventType);
                    if(delayBetweenEvents!=null) {
                        try {
                            LOG.info("Sleeping after producing {}", eventType);
                            Thread.sleep(delayBetweenEvents.toMillis());
                        } catch (InterruptedException ex) {
                            ex.printStackTrace();
                        }
                    }
                }
                counter++;
            }
        };
        Future<?> submit = executorService.submit(generateTask);
        submit.get();
    }
}
