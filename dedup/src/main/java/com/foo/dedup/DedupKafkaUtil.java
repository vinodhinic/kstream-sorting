package com.foo.dedup;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public final class DedupKafkaUtil {

    private static final Logger LOG = LoggerFactory.getLogger(DedupKafkaUtil.class);

    private static Properties inputTopicConsumerProperties = new Properties();
    private static Properties producerProperties = new Properties();
    private static String consumerGroup = "dedup-input-consumer-group";

    static {
        inputTopicConsumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        inputTopicConsumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        // todo: enable this once msk is upgrade to kafka version 2.3.0 or above
        // Group instance id is to uniquely identify a consumer within a group.
        // put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, consumerGroup)
        inputTopicConsumerProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, consumerGroup);

        // when consumer is seeked to an offset that is non-existent, don't seek to earliest/latest. Just do nothing.
        inputTopicConsumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");

        // Read only the messages that are committed by the upstream
        inputTopicConsumerProperties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        // since we take full control on when to commit a message as "consumed" auto-commit should be set to false
        inputTopicConsumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        // max records that can be returned by consumer.poll()
        int maxRecordsPerKafkaConsumerPoll = 1000;
        inputTopicConsumerProperties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxRecordsPerKafkaConsumerPoll);
        LOG.info("Kafka consumer configured with max records per consumer poll :" + maxRecordsPerKafkaConsumerPoll);

        inputTopicConsumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, new StringDeserializer().getClass().getName());
        inputTopicConsumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, new StringDeserializer().getClass().getName());
        inputTopicConsumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        producerProperties.put(ProducerConfig.CLIENT_ID_CONFIG, "dedup-producer");
        // **** Enforcing exactly once semantics ****
        // Enforce ack from all brokers to enforce strong producer guarantees
        producerProperties.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProperties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        // transaction.id should be unique for a producer. Since we have one MSK and topics created per client, it is important for producer to identify a transaction per client
        producerProperties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "dedup-producer");
        producerProperties.put(ProducerConfig.RETRIES_CONFIG, 3);
        // max in flight request set to 1 ensures that messages are delivered in order even when there are retries between transactions.
        producerProperties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        // *******************************************
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, new StringSerializer().getClass().getName());
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, new StringSerializer().getClass().getName());
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    }

    private DedupKafkaUtil() {
    }

    public static String getConsumerGroupId() {
        return consumerGroup;
    }


    public static Properties getInputTopicConsumerProperties() {
        return inputTopicConsumerProperties;
    }

    public static Properties getProducerProperties() {
        return producerProperties;
    }
}
