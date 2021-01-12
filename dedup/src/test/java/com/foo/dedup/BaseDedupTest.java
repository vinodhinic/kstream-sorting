package com.foo.dedup;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.util.SocketUtils;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

import static com.foo.dedup.DedupTestConfig.*;

@ActiveProfiles("test")
@SpringJUnitConfig(classes = {DedupStoreConfiguration.class, DedupTestConfig.class})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
public abstract class BaseDedupTest {

    private static final Logger LOG = LoggerFactory.getLogger(BaseDedupTest.class);

    public static EmbeddedKafkaRule embeddedKafkaRule =
            new EmbeddedKafkaRule(1, true, 1, INPUT_TOPIC, OUTPUT_TOPIC, TEST_TOPIC);

    // override producer and consumer property for test setup.
    static {
        Properties inputTopicConsumerProperties = DedupKafkaUtil.getInputTopicConsumerProperties();
        // Setting greater session timeout and also, should also be greater than heartbeat
        inputTopicConsumerProperties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 8000);
        // setting batch record to 2 to simulate kickout within a batch
        inputTopicConsumerProperties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 2);

        Properties producerProperties = DedupKafkaUtil.getProducerProperties();
        producerProperties.put(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG, 1000);

        try {
            Path path = Paths.get(TEST_ROCKS_DIR);
            LOG.info("Previous test run might not have cleared properly. Removing existing rocks");
            FilesUtil.removeDirectory(path);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    protected EmbeddedKafkaBroker kafka;

    protected void initializeEmbeddedKafka() {
        int[] ports = SocketUtils.findAvailableTcpPorts(1).stream().mapToInt(e -> e).toArray();
        embeddedKafkaRule.kafkaPorts(ports);
        // important to override these properties for test (because test is running with 1 broker)
        embeddedKafkaRule.brokerProperty("transaction.state.log.min.isr", 1);
        embeddedKafkaRule.brokerProperty(
                "transaction.state.log.replication.factor", Short.valueOf("1"));
        embeddedKafkaRule.brokerProperty("log.dirs", "/home/chockali/kafka-test-logs");
        String brokersAsString = embeddedKafkaRule.getEmbeddedKafka().getBrokersAsString();
        LOG.info("Embedded kafka broker URL : {}", brokersAsString);
        embeddedKafkaRule.before();

        String brokerUrl = embeddedKafkaRule.getEmbeddedKafka().getBrokersAsString();
        DedupKafkaUtil.getProducerProperties().put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerUrl);
        DedupKafkaUtil.getInputTopicConsumerProperties()
                .put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerUrl);
    }
}
