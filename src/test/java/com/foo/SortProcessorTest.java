package com.foo;

import com.foo.model.Event;
import com.foo.model.EventType;
import com.foo.util.FooSerdes;
import com.foo.util.MockEventDataProducer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.function.Function;

import static com.foo.App.bufferIntervalInSeconds;
import static com.foo.App.stateStoreName;
import static com.foo.util.MockEventDataProducer.EVENTS_TOPIC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class SortProcessorTest {

    private TopologyTestDriver topologyTestDriver;
    private StringDeserializer stringDeserializer = new StringDeserializer();
    private Deserializer<Event> eventDeserializer = new FooSerdes.EventSerde().deserializer();

    private ConsumerRecordFactory<String, Event> recordFactory =
            new ConsumerRecordFactory<>(new StringSerializer(), new FooSerdes.EventSerde().serializer());

    @Before
    public void before() {
        topologyTestDriver = new TopologyTestDriver(App.topology(), App.getConfig());
    }

    @Test
    public void testSortProcessor() {

        Function<Event, String> eventKey = e -> e.getEventType() + " " + e.getId();
        Function<Event, Event> eventWithUpperCasePayload = e -> Event.builder(e).payload(e.getPayload().toUpperCase()).build();

        Event randomEvent1 = MockEventDataProducer.getRandomEvent(EventType.C);
        produceIntoSourceTopic(randomEvent1);

        Event randomEvent2 = MockEventDataProducer.getRandomEvent(EventType.A);
        produceIntoSourceTopic(randomEvent2);

        KeyValueStore<String, Event> keyValueStore = topologyTestDriver.getKeyValueStore(stateStoreName);
        assertEquals(eventWithUpperCasePayload.apply(randomEvent1), keyValueStore.get(eventKey.apply(randomEvent1)));
        assertEquals(eventWithUpperCasePayload.apply(randomEvent2), keyValueStore.get(eventKey.apply(randomEvent2)));
        assertEquals(2, keyValueStore.approximateNumEntries());

        topologyTestDriver.advanceWallClockTime(Duration.ofSeconds(bufferIntervalInSeconds + 1).toMillis());
        assertEquals(keyValueStore.approximateNumEntries(), 0);

        ProducerRecord<String, Event> firstRecordRead = readFromSinkTopic();
        assertNull(firstRecordRead.key());
        assertEquals(eventWithUpperCasePayload.apply(randomEvent2), firstRecordRead.value());

        ProducerRecord<String, Event> secondRecordRead = readFromSinkTopic();
        assertNull(secondRecordRead.key());
        assertEquals(eventWithUpperCasePayload.apply(randomEvent1), secondRecordRead.value());
    }

    private ProducerRecord<String, Event> readFromSinkTopic() {
        return topologyTestDriver.readOutput(EventDataConsumer.SINK_TOPIC_NAME,
                stringDeserializer,
                eventDeserializer);
    }

    private void produceIntoSourceTopic(Event randomEvent1) {
        ConsumerRecord<byte[], byte[]> consumerRecord = recordFactory.create(EVENTS_TOPIC,
                null, randomEvent1);
        topologyTestDriver.pipeInput(consumerRecord);
    }
}
