package com.foo.dedup;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.*;

@Component
public class DedupRunner {

    private static final Logger LOG = LoggerFactory.getLogger(DedupRunner.class);

    private final Object lock = new Object();
    private volatile boolean terminated = false;

    @Autowired
    @Qualifier("outputTopic")
    private String outputTopic;

    @Autowired
    @Qualifier("inputTopic")
    private String inputTopic;

    @Autowired
    private Path rocksPath;

    @Autowired
    private DuplicateKickoutLogger duplicateKickoutLogger;

    private DedupRunnable dedupRunnable;
    private Future<?> handleToDedupRunnable;
    private ExecutorService executorService;

    public synchronized void start() throws IOException, RocksDBException {
        this.dedupRunnable =
                new DedupRunnable(inputTopic, outputTopic, rocksPath,
                        duplicateKickoutLogger);
        executorService =
                Executors.newSingleThreadExecutor(
                        new ThreadFactoryBuilder().setNameFormat("dedup-runner-%d").build());
        handleToDedupRunnable = executorService.submit(dedupRunnable);
        Runtime.getRuntime()
                .addShutdownHook(
                        new Thread(
                                () -> {
                                    try {
                                        LOG.info("Shutdown Hook invoked..");
                                        stop();
                                    } catch (InterruptedException e) {
                                        LOG.error("Exception while stopping the Dedup Runner", e);
                                    }
                                }));
        LOG.info("DedupRunner started");
    }

    public boolean isRunning() {
        return dedupRunnable.isRunning();
    }

    public void stop() throws InterruptedException {
        LOG.info("Attempting to stop DedupRunner...");
        this.dedupRunnable.shutdown();
        this.executorService.shutdown();
        if (!this.executorService.awaitTermination(60, TimeUnit.SECONDS)) {
            LOG.info("Grace period exceeded. DedupRunner shutting down executor service");
            this.executorService.shutdownNow();
        } else {
            LOG.info("DedupRunner gracefully stopped");
        }
        this.dedupRunnable.cleanup();
        this.terminated = true;
    }

    public void blockUntilShutdown() throws InterruptedException, ExecutionException {
        // block on this future to catch the Execution exception and let the main thread invoke SIGTERM
        // for the shutdown hook to kick in
        handleToDedupRunnable.get();
        synchronized (lock) {
            while (!terminated) {
                lock.wait();
            }
        }
        LOG.info("DedupRunner shutting down");
    }
}
