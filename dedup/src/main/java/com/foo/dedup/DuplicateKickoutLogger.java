package com.foo.dedup;

// A dummy interface that is useful to assert duplicates in test cases
public interface DuplicateKickoutLogger {
    void log(String kafkaMessage);
}
