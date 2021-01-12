package com.foo.dedup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class DuplicateKickoutLoggerImpl implements DuplicateKickoutLogger {

    private static final Logger LOG = LoggerFactory.getLogger(DuplicateKickoutLoggerImpl.class);

    @Override
    public void log(String kafkaMessage) {
        LOG.error("Duplicate found {}", kafkaMessage);
    }
}
