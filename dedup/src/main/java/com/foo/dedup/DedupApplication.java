package com.foo.dedup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class DedupApplication {
    private static final Logger LOG = LoggerFactory.getLogger(DedupRunner.class);

    public static void main(String[] args) {
        try (AnnotationConfigApplicationContext context =
                     new AnnotationConfigApplicationContext(DedupStoreConfiguration.class)) {
            DedupRunner dedupRunner = context.getBean(DedupRunner.class);
            dedupRunner.start();
            dedupRunner.blockUntilShutdown();
        } catch (Exception e) {
            LOG.error("Dedup application threw exception {}. Exiting.", e.getMessage(), e);
            System.exit(1);
        }
    }

}
