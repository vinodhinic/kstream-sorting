package com.foo.dedup;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DedupRunnable implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(DedupRunnable.class);
    private static final Duration MAX_DURATION_TO_BLOCK_ON_EMPTY_MESSAGE = Duration.ofSeconds(2);
    private final TopicPartition inputTopicPartition;
    private final KafkaConsumer<String, String> inputTopicConsumer;
    private final KafkaProducer<String, String> dedupTopicProducer;
    private final DedupStore dedupStore;
    private final String outputTopic;
    private final DuplicateKickoutLogger duplicateKickoutLogger;
    private volatile boolean isTerminated = false;
    private volatile boolean isRunning = false;


    public DedupRunnable(
            String inputTopic,
            String outputTopic,
            Path rocksPath,
            DuplicateKickoutLogger duplicateKickoutLogger)
            throws IOException, RocksDBException {
        // Important for dedupStore initialization to be the first line of initialization here. Rocks
        // can be opened for write by only one thread. This promises the idempotency we want.
        this.dedupStore = new DedupStore(RocksUtil.initRocksDb(rocksPath));
        this.outputTopic = outputTopic;
        this.duplicateKickoutLogger = duplicateKickoutLogger;
        dedupTopicProducer = new KafkaProducer<>(DedupKafkaUtil.getProducerProperties());
        inputTopicConsumer = new KafkaConsumer<>(DedupKafkaUtil.getInputTopicConsumerProperties());
        LOG.info("Input topic initialized {}. Output topic initialized {} ", inputTopic, outputTopic);
        List<PartitionInfo> partitionsForTopic = inputTopicConsumer.partitionsFor(inputTopic);
        if (partitionsForTopic.size() != 1) {
            throw new RuntimeException(
                    "Expected only one partition for the topic \""
                            + inputTopic
                            + "\" but found \""
                            + partitionsForTopic.size()
                            + "\"");
        }

        inputTopicConsumer.assign(
                partitionsForTopic
                        .stream()
                        .map(info -> new TopicPartition(info.topic(), info.partition()))
                        .collect(Collectors.toList()));

        PartitionInfo info = inputTopicConsumer.partitionsFor(inputTopic).get(0);
        inputTopicPartition = new TopicPartition(info.topic(), info.partition());

        // Ensure there are no ongoing transactions before consuming "read committed" off of
        // dedup-topic in recovery
        dedupTopicProducer.initTransactions();
    }

    @Override
    public void run() {
        LOG.info("Started DedupRunnable...");
        isRunning = true;
        init();
        // TODO: Find out if eager consumer registration with broker is happening or it happens only
        // during first time poll.
        while (!isTerminated) {
            LOG.trace("DedupRunnable polling...");
            ConsumerRecords<String, String> consumerRecords =
                    inputTopicConsumer.poll(MAX_DURATION_TO_BLOCK_ON_EMPTY_MESSAGE);
            int batchSize = consumerRecords.count();
            if (batchSize == 0) {
                continue;
            }
            LOG.info(
                    "Consumed {} events in single poll. Consumer is at position : {}",
                    batchSize,
                    inputTopicConsumer.position(inputTopicPartition));
            Iterator<ConsumerRecord<String, String>> recordIterator =
                    consumerRecords.iterator();
            String consumedEvent = null;
            try {
                Instant start = Instant.now();
                dedupTopicProducer.beginTransaction();
                OffsetAndMetadata latestOffsetMetadata = null;
                while (recordIterator.hasNext()) {
                    ConsumerRecord<String, String> next = recordIterator.next();
                    consumedEvent = next.value();
                    long currentOffset = next.offset();
                    boolean isDuplicate =
                            dedupStore.putIfAbsentAndGetIsDuplicate(next.key().getBytes(), currentOffset);
                    if (isDuplicate) {
                        LOG.info(
                                "DedupStore evaluated {} at offset {} to duplicate",
                                consumedEvent,
                                currentOffset);
                        duplicateKickoutLogger.log(consumedEvent);
                    } else {
                        LOG.debug(
                                "DedupStore evaluated {} at offset {} to non-duplicate",
                                consumedEvent,
                                currentOffset);
                        ProducerRecord<String, String> record =
                                new ProducerRecord<>(outputTopic, consumedEvent);
                        dedupTopicProducer.send(record);
                    }
                    latestOffsetMetadata = new OffsetAndMetadata(currentOffset);
                }
                dedupTopicProducer.sendOffsetsToTransaction(
                        Map.of(inputTopicPartition, latestOffsetMetadata), DedupKafkaUtil.getConsumerGroupId());
                dedupTopicProducer.commitTransaction();
                LOG.info(
                        "Total time took to process {} events : {} micros",
                        batchSize,
                        ChronoUnit.MICROS.between(start, Instant.now()));
            } catch (Throwable e) {
                /* important not to suppress the exception. The next restart will recover from last committed offset */
                String errorMessage =
                        "Unable to dedup the last batch of size "
                                + batchSize
                                + ". Failed while processing event "
                                + (consumedEvent != null ? consumedEvent : null);
                LOG.error(errorMessage, e);
                isRunning = false;
                dedupTopicProducer.abortTransaction();
                throw new IllegalStateException(errorMessage, e);
            }
        }
        isRunning = false;
    }

    private void init() {
        OffsetAndMetadata lastCommittedOffset = inputTopicConsumer.committed(inputTopicPartition);
        if (lastCommittedOffset == null) {
            LOG.info("Dedup store is initialized for client for the first time");
            inputTopicConsumer.seekToBeginning(Collections.singletonList(inputTopicPartition));
        } else {
            LOG.info(
                    "Seeking to the last committed offset {} + 1 = {}",
                    lastCommittedOffset.offset(),
                    lastCommittedOffset.offset() + 1);
            inputTopicConsumer.seek(inputTopicPartition, lastCommittedOffset.offset() + 1);
        }
    }

    public boolean isRunning() {
        return isRunning;
    }

    public void shutdown() {
        this.isTerminated = true;
    }

    public void cleanup() {
        LOG.info("Dedup kafka resources are getting cleaned up..");
        dedupTopicProducer.flush();
        dedupTopicProducer.close();
        inputTopicConsumer.close();
        LOG.info("Closing dedup rocks");
        dedupStore.close();
    }
}
