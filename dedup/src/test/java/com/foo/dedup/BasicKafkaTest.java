package com.foo.dedup;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.foo.dedup.DedupTestConfig.TEST_TOPIC;
import static org.junit.jupiter.api.Assertions.*;

public class BasicKafkaTest extends BaseDedupTest {
    private static final Logger LOG = LoggerFactory.getLogger(BasicKafkaTest.class);
    private String topic = TEST_TOPIC;

    private KafkaProducer<String, String> testProducer;
    private KafkaConsumer<String, String> testConsumer;
    private TopicPartition topicPartition;

    @BeforeEach
    public void setup() {
        initializeEmbeddedKafka();
        kafka = embeddedKafkaRule.getEmbeddedKafka();
        Assertions.assertNotNull(kafka);
        Assertions.assertTrue(
                kafka.getTopics().contains(topic), "Expected " + topic + " in " + kafka.getTopics());

        // initialize input topic producer
        Map<String, Object> producerProps = KafkaTestUtils.producerProps(kafka);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "test-topic-producer");
        producerProps.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        producerProps.put(ProducerConfig.RETRIES_CONFIG, 1);

        testProducer = new KafkaProducer<>(producerProps);
        testProducer.initTransactions();
        // initialize output topic consumer. This is downstream of output topic
        Map<String, Object> consumerProps =
                KafkaTestUtils.consumerProps("dedup-test-consumer", "true", kafka);
        consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        consumerProps.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // try changing this property to none.
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        testConsumer = new KafkaConsumer<>(consumerProps);
        List<PartitionInfo> partitionsForTopic = testConsumer.partitionsFor(topic);
        if (partitionsForTopic.size() != 1) {
            throw new RuntimeException(
                    "Expected only one partition for the topic \""
                            + topic
                            + "\" but found \""
                            + partitionsForTopic.size()
                            + "\"");
        }

        testConsumer.assign(
                partitionsForTopic
                        .stream()
                        .map(info -> new TopicPartition(info.topic(), info.partition()))
                        .collect(Collectors.toList()));

        PartitionInfo info = testConsumer.partitionsFor(topic).get(0);
        topicPartition = new TopicPartition(info.topic(), info.partition());
    }

    @Test
    public void test() throws ExecutionException, InterruptedException {
        produce("IBM");
        produce("APPLE");
        produce("MFST");

        long offset = -1;
        CountDownLatch latch = new CountDownLatch(3);
        while (latch.getCount() != 0) {
            ConsumerRecords<String, String> consumerRecords = testConsumer.poll(Duration.ofSeconds(10));
            LOG.info("Last poll has given {} records", consumerRecords.count());
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                latch.countDown();
                if (consumerRecord.value().equals("APPLE")) {
                    offset = consumerRecord.offset();
                }
                LOG.info(
                        "Consumed event {} with offset {} and consumer is at position {} ",
                        consumerRecord.value(),
                        consumerRecord.offset(),
                        testConsumer.position(topicPartition));
            }
        }
        assertTrue(latch.await(10, TimeUnit.SECONDS));
        assertNotEquals(offset, -1);
        testConsumer.seek(topicPartition, offset);
        LOG.info("After seeking to offset {} ", offset);
        CountDownLatch latch2 = new CountDownLatch(1);
        while (latch2.getCount() != 0) {
            int count = pollAndPrint();
            for (int i = 0; i < count; i++) {
                latch2.countDown();
            }
        }
        assertTrue(latch2.await(10, TimeUnit.SECONDS));

        LOG.info("After seeking to a non-existent offset..");
        testConsumer.seek(topicPartition, 10); // seeking to non-existent offset
        pollAndPrint(); // due to earliest auto reset, this will print from IBM
    }

    private int pollAndPrint() {
        int count = 0;
        ConsumerRecords<String, String> consumerRecords = testConsumer.poll(Duration.ofSeconds(10));
        LOG.info("Last poll has given {} records", consumerRecords.count());
        for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
            count++;
            LOG.info(
                    "Consumed event {} with offset {} and consumer is at position {} ",
                    consumerRecord.value(),
                    consumerRecord.offset(),
                    testConsumer.position(topicPartition));
        }
        return count;
    }

    private void produce(String message) throws ExecutionException, InterruptedException {
        testProducer.beginTransaction();

        Future<RecordMetadata> recordMetadataFuture =
                this.testProducer.send(new ProducerRecord<>(topic, message));
        testProducer.commitTransaction();
        RecordMetadata recordMetadata = recordMetadataFuture.get();
        assertNotNull(recordMetadata);
        assertEquals(topic, recordMetadata.topic());
    }

    @AfterEach
    public void cleanup() {
        testProducer.close();
        testConsumer.close();
        embeddedKafkaRule.after();
    }
}

