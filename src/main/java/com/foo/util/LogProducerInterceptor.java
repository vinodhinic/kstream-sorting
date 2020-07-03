package com.foo.util;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class LogProducerInterceptor<K, V> implements ProducerInterceptor<K, V> {

    private static final Logger LOG = LoggerFactory.getLogger(LogProducerInterceptor.class);

    @Override
    public ProducerRecord<K, V> onSend(ProducerRecord<K, V> record) {
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        LOG.info("Ack received for {}", metadata);
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {
        LOG.info("Producer Config : {}", configs);
    }
}
