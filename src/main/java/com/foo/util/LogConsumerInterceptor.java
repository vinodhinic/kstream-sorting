package com.foo.util;

import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class LogConsumerInterceptor<K, V> implements ConsumerInterceptor<K, V> {

    private static final Logger LOG = LoggerFactory.getLogger(LogConsumerInterceptor.class);

    @Override
    public ConsumerRecords<K, V> onConsume(ConsumerRecords<K, V> records) {
        return records;
    }

    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        // This does not work in EOS enabled. When EOS is enabled, producer.sendOffsetsToTransaction is called.
        // consumerInterceptors.onCommit gets called only when kafkaConsumer.commit is called.
        // https://stackoverflow.com/questions/62700075/is-there-a-way-to-get-committed-offset-in-eos-kafka-stream
        LOG.info("Committing {} to source topic ", offsets);
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {
        LOG.info("Consumer Config : {}",configs);
    }
}
