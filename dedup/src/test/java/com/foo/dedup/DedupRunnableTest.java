package com.foo.dedup;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

public class DedupRunnableTest extends BaseDedupTest {

    private static final Logger LOG = LoggerFactory.getLogger(DedupRunnableTest.class);
    private static final long TEST_TIMEOUT_IN_SECONDS = 60;
    private static final Duration MAX_DURATION_TO_BLOCK_ON_EMPTY_MESSAGE = Duration.ofSeconds(2);
    @Autowired
    private DedupRunner dedupRunner;

    @Autowired
    @Qualifier("outputTopic")
    private String outputTopic;

    @Autowired
    @Qualifier("inputTopic")
    private String inputTopic;

    @Autowired
    @Qualifier("rocksPath")
    private Path rocksPath;

    @Autowired
    private DuplicateKickoutLogger duplicateKickoutPublisher;

    private KafkaProducer<String, String> testInputTopicProducer;
    private KafkaConsumer<String, String> testDedupConsumer;

    @BeforeEach
    public void setup() {
        initializeEmbeddedKafka();
        kafka = embeddedKafkaRule.getEmbeddedKafka();
        Assertions.assertNotNull(kafka);
        Assertions.assertTrue(
                kafka.getTopics().contains(inputTopic),
                "Expected " + inputTopic + " in " + kafka.getTopics());
        Assertions.assertTrue(
                kafka.getTopics().contains(outputTopic),
                "Expected " + outputTopic + " in " + kafka.getTopics());

        // initialize input topic producer
        Map<String, Object> producerProps = KafkaTestUtils.producerProps(kafka);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "test-input-topic-producer");
        producerProps.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        producerProps.put(ProducerConfig.RETRIES_CONFIG, 1);

        testInputTopicProducer = new KafkaProducer<>(producerProps);
        testInputTopicProducer.initTransactions();
        // initialize output topic consumer. This is like mock component that comes after this dedup compoent in the pipeline
        Map<String, Object> consumerProps =
                KafkaTestUtils.consumerProps("dedup-test-consumer", "true", kafka);
        consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        testDedupConsumer = new KafkaConsumer<>(consumerProps);
        testDedupConsumer.subscribe(Collections.singleton(outputTopic));
    }

    @Test
    public void testSeekToLastCommittedOffsetWhenThereIsNoMoreEventsToConsume() {

        assertTimeout(
                Duration.ofSeconds(TEST_TIMEOUT_IN_SECONDS),
                () -> {
                    String book1 = produceBookInsertEvent("1");
                    String book2 = produceBookInsertEvent("2");

                    dedupRunner.start();

                    assertThatEventsMadeItToDedupTopic(2, List.of(book1, book2));

                    // stop and start should ensure that the events are read from the last committed
                    dedupRunner.stop();

                    dedupRunner.start();

                    // wait for the dedupRunner to be polling. This should ensure that the seek to a
                    // non-existent offset is not breaking anything.
                    while (!dedupRunner.isRunning()) {
                        Thread.sleep(100);
                    }

                    String book3 = produceBookInsertEvent("3");
                    assertThatEventsMadeItToDedupTopic(1, List.of(book3));
                });
    }

    @Test
    public void testHappyPath() {
        assertTimeout(
                Duration.ofSeconds(TEST_TIMEOUT_IN_SECONDS),
                () -> {
                    String book1 = produceBookInsertEvent("1");
                    String book2 = produceBookInsertEvent("2");
                    String book2Duplicate = produceBookInsertEvent("2");
                    String book7 = produceBookInsertEvent("7");
                    dedupRunner.start();

                    assertThatEventsMadeItToDedupTopic(3, List.of(book1, book2, book7));

                    ArgumentCaptor<String> argumentCaptor =
                            ArgumentCaptor.forClass(String.class);
                    Mockito.verify(duplicateKickoutPublisher).log(argumentCaptor.capture());
                    String duplicateEventKickedout = argumentCaptor.getValue();
                    assertNotNull(duplicateEventKickedout);
                    assertEquals(book2Duplicate, duplicateEventKickedout);

                    // stop and start should ensure that the events are read from the last committed
                    dedupRunner.stop();

                    String book3 = produceBookInsertEvent("3");
                    dedupRunner.start();
                    assertThatEventsMadeItToDedupTopic(1, List.of(book3));
                });
    }

    @Test
    public void testWhenABatchIsFullOfKickouts() {
        assertTimeout(
                Duration.ofSeconds(TEST_TIMEOUT_IN_SECONDS),
                () -> {
                    // batch : 1
                    String book1 = produceBookInsertEvent("1");
                    String book1Duplicate1 = produceBookInsertEvent("1");
                    // batch : 2
                    String book2 = produceBookInsertEvent("2");
                    String book3 = produceBookInsertEvent("3");
                    // batch : 3
                    String book1Duplicate2 = produceBookInsertEvent("1");
                    String book1Duplicate3 = produceBookInsertEvent("1");
                    // batch : 4
                    String book4 = produceBookInsertEvent("4");
                    String book3Duplicate = produceBookInsertEvent("3");

                    dedupRunner.start();

                    // Note that this consumer will see book4 only when the complete batch 4 is committed
                    // since it is running with read_committed isolation
                    assertThatEventsMadeItToDedupTopic(4, List.of(book1, book2, book3, book4));

                    ArgumentCaptor<String> argumentCaptor =
                            ArgumentCaptor.forClass(String.class);

                    Mockito.verify(duplicateKickoutPublisher, Mockito.times(4))
                            .log(argumentCaptor.capture());
                    List<String> duplicateEventsKickedout = argumentCaptor.getAllValues();
                    assertNotNull(duplicateEventsKickedout);
                    assertEquals(4, duplicateEventsKickedout.size());
                    assertTrue(
                            duplicateEventsKickedout.containsAll(
                                    List.of(book1Duplicate1, book1Duplicate2, book1Duplicate3, book3Duplicate)));
                });
    }

    @Test
    public void testDedupServerIdempotency() throws IOException, RocksDBException {
        dedupRunner.start();
        assertThrows(RocksDBException.class, () -> dedupRunner.start());
    }

    @Test
    public void testTransactionFailureForABatch() {
        assertTimeout(
                Duration.ofSeconds(TEST_TIMEOUT_IN_SECONDS),
                () -> {
                    String book1 = produceBookInsertEvent("1");
                    String book2 = produceBookInsertEvent("2");
                    String book1Duplicate = produceBookInsertEvent("1");
                    String book3 = produceBookInsertEvent("3");

                    // Throwing exception to cause dedupServer to crash.
                    Mockito.doThrow(IllegalArgumentException.class)
                            .when(duplicateKickoutPublisher)
                            .log(Mockito.any());

                    dedupRunner.start();
                    CountDownLatch latchForFailure = new CountDownLatch(1);
                    Executors.newSingleThreadExecutor(
                            new ThreadFactoryBuilder().setNameFormat("dedup-runnable-test-%d").build())
                            .submit(
                                    () -> {
                                        try {
                                            dedupRunner.blockUntilShutdown();
                                        } catch (InterruptedException | ExecutionException e) {
                                            latchForFailure.countDown();
                                        }
                                    });
                    assertTrue(latchForFailure.await(TEST_TIMEOUT_IN_SECONDS, TimeUnit.SECONDS));

                    // stop and start should ensure that the events are read from the last committed
                    dedupRunner.stop();

                    // Second run is simulated to not throw any exceptions.
                    Mockito.reset(duplicateKickoutPublisher);

                    dedupRunner.start();

                    assertThatEventsMadeItToDedupTopic(3, List.of(book1, book2, book3));

                    ArgumentCaptor<String> argumentCaptor =
                            ArgumentCaptor.forClass(String.class);
                    Mockito.verify(duplicateKickoutPublisher).log(argumentCaptor.capture());
                    String duplicateEventKickedout = argumentCaptor.getValue();
                    assertNotNull(duplicateEventKickedout);
                    assertEquals(book1Duplicate, duplicateEventKickedout);
                });
    }

    private void assertThatEventsMadeItToDedupTopic(
            int expectedEventCount, List<String> expectedEvents)
            throws InterruptedException {

        final CountDownLatch latch = new CountDownLatch(expectedEventCount);
        List<String> consumedEvents = new ArrayList<>();
        Executors.newSingleThreadExecutor(
                new ThreadFactoryBuilder().setNameFormat("dedup-runnable-test-%d").build())
                .submit(
                        () -> {
                            while (latch.getCount() != 0) {
                                LOG.debug("Polling until I get {} records...", expectedEventCount);
                                ConsumerRecords<String, String> consumerRecords =
                                        testDedupConsumer.poll(MAX_DURATION_TO_BLOCK_ON_EMPTY_MESSAGE);
                                for (ConsumerRecord<String, String> dedupedEvent :
                                        consumerRecords) {
                                    consumedEvents.add(dedupedEvent.value());
                                    latch.countDown();
                                    assertNotNull(dedupedEvent);
                                }
                            }
                            LOG.debug("Consumed {} events off of dedup-topic", expectedEventCount);
                        });
        assertTrue(latch.await(TEST_TIMEOUT_IN_SECONDS, TimeUnit.SECONDS));

        assertTrue(
                expectedEvents.containsAll(consumedEvents),
                "Expected " + expectedEvents + " but got " + consumedEvents);
    }

    private String produceBookInsertEvent(String bookId)
            throws ExecutionException, InterruptedException {
        testInputTopicProducer.beginTransaction();

        Future<RecordMetadata> recordMetadataFuture =
                this.testInputTopicProducer.send(new ProducerRecord<>(inputTopic, bookId, bookId));
        testInputTopicProducer.commitTransaction();
        RecordMetadata recordMetadata = recordMetadataFuture.get();
        LOG.trace("Produced event into input-topic {} ", bookId);
        assertNotNull(recordMetadata);
        assertEquals(inputTopic, recordMetadata.topic());
        return bookId;
    }

    @AfterEach
    public void cleanup() throws InterruptedException {
        testInputTopicProducer.close();
        testDedupConsumer.close();
        LOG.debug("Tearing down dedup runner");
        dedupRunner.stop();
        LOG.debug("Tearing down embedded kafka");
        embeddedKafkaRule.after();
        LOG.debug("Deleting dedup rocks");
        try {
            FilesUtil.removeDirectory(rocksPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
