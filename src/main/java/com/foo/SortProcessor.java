package com.foo;

import com.foo.model.Event;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

public class SortProcessor extends AbstractProcessor<String, Event> {

    private static Logger LOG = LoggerFactory.getLogger(SortProcessor.class);
    private final String stateStore;
    private final Long bufferIntervalInSeconds;

    // Why not use a simple Java NavigableMap? Check out my answer at : https://stackoverflow.com/a/62677079/2256618
    private KeyValueStore<String, Event> keyValueStore;

    public SortProcessor(String stateStore, Long bufferIntervalInSeconds) {
        this.stateStore = stateStore;
        this.bufferIntervalInSeconds = bufferIntervalInSeconds;
    }

    @Override
    public void init(ProcessorContext processorContext) {
        super.init(processorContext);
        keyValueStore = (KeyValueStore) context().getStateStore(stateStore);
        context().schedule(Duration.ofSeconds(bufferIntervalInSeconds), PunctuationType.WALL_CLOCK_TIME, this::punctuate);
    }

    void punctuate(long timestamp) {
        LOG.info("Punctuator invoked...");
        try(KeyValueIterator<String, Event> iterator = keyValueStore.all()) {
            while (iterator.hasNext()) {
                KeyValue<String, Event> next = iterator.next();
                if(next.value == null) {
                    continue;
                }
                LOG.info("Sending {}", next.key);
                // sleep();
                context().forward(next.key, next.value);
                // iterator.remove(); - this does not work since the iterator returned is read only
                keyValueStore.delete(next.key);
            }
        }
        // uncomment to test resilience
        // throwException();
    }

    private void throwException() {
        if(true) {
            throw new IllegalStateException("Intentional");
        }
    }

    private void sleep() {
        try {
            LOG.info("Sleeping for 3 seconds");
            Thread.sleep(3 * 1000);
            LOG.info("Waking up after 3 seconds");
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

    @Override
    public void process(String key, Event value) {
        Event event = Event.builder(value).payload(value.getPayload().toUpperCase()).build();
        LOG.info("Updating " + keyValueStore.hashCode() + " and processor : " + this.hashCode() +  " with " + key);
        keyValueStore.put(event.getEventType().name(), event);
    }

    public static String getName() {
        return "sort-processor";
    }
}
