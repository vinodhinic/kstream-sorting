package com.foo.dedup;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

@Configuration
@ComponentScan(basePackages = {"com.foo.dedup"})
public class DedupStoreConfiguration {
    private static final Logger LOG = LoggerFactory.getLogger(DedupStoreConfiguration.class);

    @Bean("rocksPath")
    public Path rocksPath() throws IOException {
        return Files.createTempDirectory("/dedup");
    }

    @Bean("inputTopic")
    public String getInputTopic() {
        return "input-topic";
    }

    @Bean("outputTopic")
    public String getOutputTopic() {
        return "output-topic";
    }

}
