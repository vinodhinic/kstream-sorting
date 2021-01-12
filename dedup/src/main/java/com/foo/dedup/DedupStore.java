package com.foo.dedup;

import com.foo.dedup.Dedup.DedupValue;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;
import com.google.protobuf.UInt64Value;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

public class DedupStore {

    private static Logger LOG = LoggerFactory.getLogger(DedupStore.class);

    private final RocksDB db;
    private final ColumnFamilyHandle col; // Default column family

    // NOTE: To achieve good performance, the Rocks write buffer must be >= 75MB.
    public DedupStore(
            RocksDB db) {
        this.db = db;
        this.col = db.getDefaultColumnFamily();
        // Setup sequential executor per shard. Approx 5-10 shards.
    }

    // Not thread-safe as implemented. See notes for how to do that.
    public synchronized boolean putIfAbsentAndGetIsDuplicate(final byte[] keyBytes, long offsetId)
            throws RocksDBException, InvalidProtocolBufferException {
        // Calculate the shard_id using a hash function on keyBytes.
        // Send the code block below to the sequential executor for shard_id, returning
        // the ListenableFuture<Boolean>.
        // When the above is complete, this function need not be synchronized because the sequential
        // executor by shard_id avoids races.
        boolean isDuplicate;
        Instant start = Instant.now();
        StringBuilder value = new StringBuilder();
        if (db.keyMayExist(col, keyBytes, value)) {
            byte[] val = db.get(col, keyBytes);
            if (val != null) {
                Dedup.DedupValue dedupValue = Dedup.DedupValue.parseFrom(val);
                if (dedupValue.getOffsetId().getValue() == offsetId) {
                    isDuplicate = false;
                    LOG.info(
                            "Dedup store encountered the same Kafka offset {} and returning the previous evaluation {}. "
                                    + "This happens when instance is trying to recover from failure.",
                            offsetId,
                            isDuplicate);
                } else {
                    isDuplicate = true;
                }
            } else {
                // This is a false positive. Fall-through to insert the row.
                isDuplicate = false;
                registerNewEventInDedupStore(keyBytes, offsetId);
            }
        } else {
            isDuplicate = false;
            registerNewEventInDedupStore(keyBytes, offsetId);
        }

        LOG.debug(
                "Took {} micros to evaluate {} in rocks ",
                ChronoUnit.MICROS.between(start, Instant.now()),
                isDuplicate);
        return isDuplicate;
    }

    private void registerNewEventInDedupStore(byte[] keyBytes, long offsetId)
            throws RocksDBException {
        Dedup.DedupValue dedupValue =
                Dedup.DedupValue.newBuilder()
                        .setTimestamp(
                                Timestamp.newBuilder().setSeconds(Instant.now().getEpochSecond()).build())
                        .setOffsetId(UInt64Value.newBuilder().setValue(offsetId).build())
                        .build();
        db.put(col, keyBytes, dedupValue.toByteArray());
    }

    public Dedup.DedupValue get(byte[] dedupKeyOfLastConsumedEvent)
            throws RocksDBException, InvalidProtocolBufferException {
        byte[] val = db.get(col, dedupKeyOfLastConsumedEvent);
        if (val == null) {
            return null;
        }
        return Dedup.DedupValue.parseFrom(val);
    }

    public void close() {
        this.db.close();
    }
}
