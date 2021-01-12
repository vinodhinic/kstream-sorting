package com.foo.dedup;

import org.mockito.Mockito;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

@Profile("test")
@Configuration
public class DedupTestConfig {

    public static final String INPUT_TOPIC = "input-topic";
    public static final String OUTPUT_TOPIC = "dedup-topic";
    public static final String TEST_TOPIC = "test-topic";
    public static final String TEST_ROCKS_DIR = "dedup-test";

    @Bean("rocksPath")
    @Primary
    public Path rocksPath() throws IOException {
        return Files.createTempDirectory(TEST_ROCKS_DIR);
    }

    @Bean("inputTopic")
    @Primary
    public String getInputTopic() {
        return INPUT_TOPIC;
    }

    @Bean("outputTopic")
    @Primary
    public String getOutputTopic() {
        return OUTPUT_TOPIC;
    }

    @Bean
    @Primary
    public DuplicateKickoutLogger duplicateKickoutPublisher() {
        return Mockito.mock(DuplicateKickoutLogger.class);
    }
}
