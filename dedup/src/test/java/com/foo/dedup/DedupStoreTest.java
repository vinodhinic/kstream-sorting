package com.foo.dedup;

import com.google.protobuf.InvalidProtocolBufferException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.rocksdb.RocksDBException;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DedupStoreTest extends BaseDedupTest {

    private static int POSSIBLY_REPEATED_KEYS = 1_000;
    private static int TOTAL_INSERTS = 10_000;
    private static int PERCENT_DUPS = 5;
    private static int INTERVAL_PER_LOG = 1_000;
    private static int INTERVAL_PER_COMPACTION = 5_000;

    static {
        // Enable this to get the benchmark for large volumes - only locally. Do not set this in gitlab
        // config.
    /*
    System.setProperty(ROCKS_DB_WRITE_BUFFER_SIZE_OVERRIDE_IN_MB, "75");
    POSSIBLY_REPEATED_KEYS = 10_000;
    TOTAL_INSERTS = 10_000_000;
    PERCENT_DUPS = 5;
    INTERVAL_PER_LOG = 100_000;
    INTERVAL_PER_COMPACTION = 2_000_000;
     */
    }

    private DedupStore dedupStore;

    @Autowired
    private Path rocksPath;

    @BeforeEach
    public void setup() throws IOException, RocksDBException {
        dedupStore = new DedupStore(RocksUtil.initRocksDb(rocksPath));
    }

    @Test
    void testDeduplicationForSameOffsetId() throws InvalidProtocolBufferException, RocksDBException {
        Random random = new Random();
        byte[] randomBytes = new byte[10];
        random.nextBytes(randomBytes);

    /*
    Dedup store is working under the assumption that we would never read events out of order. Read on.
    Example : K*-$ -> Here * is the dedup key and $ is the offset. Parentheses has the value returned by dedup store.
    KA-1 (non-dup), KB-2 (non-dup), KA-2 (dup), KA-1 (non-dup)
    Final events sent  -> KA-1, KB-2, KA-1

    KA-1(non-dup), KA-1(non-dup), KA-2(dup), KA-1(non-dup)
    Final events sent  -> KA-1, KA-1, KA-1

    This is why we have enabled atomic commit for producer and consumer.
    With that kind of exactly once guarantee, you will never see KA-1 after KA-2.
    Nor will you see KA-1, KA-2, KA-1 where KA-1 is sent multiple times due to retries in Kafka

    The point is, the logic here at dedup store is tailor made for how we are processing and committing the message from kafka.
    */
        assertFalse(dedupStore.putIfAbsentAndGetIsDuplicate(randomBytes, 1));
        assertFalse(dedupStore.putIfAbsentAndGetIsDuplicate(randomBytes, 1));
        assertTrue(dedupStore.putIfAbsentAndGetIsDuplicate(randomBytes, 2));
    }

    @AfterEach
    public void cleanup() {
        dedupStore.close();
        try {
            FilesUtil.removeDirectory(rocksPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
