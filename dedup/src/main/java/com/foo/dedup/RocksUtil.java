package com.foo.dedup;

import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class RocksUtil {
    private static final Logger LOG = LoggerFactory.getLogger(RocksUtil.class);

    public static RocksDB initRocksDb(Path rocksPath) throws IOException, RocksDBException {
        if (!Files.isDirectory(rocksPath)) {
            Path directory = Files.createDirectories(rocksPath);
            LOG.info("Creating new Rocks instance at {}.", directory.toFile());
        } else {
            LOG.info("Opening existing Rocks instance at {}.", rocksPath);
        }
        List<ColumnFamilyDescriptor> columnDescriptors = new ArrayList<>();
        columnDescriptors.add(
                new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, new ColumnFamilyOptions()));

        Options options = new Options();
        BlockBasedTableConfig tableCfg = new BlockBasedTableConfig();
        tableCfg.setFilterPolicy(new BloomFilter(10, false));
        options.setTableFormatConfig(tableCfg);
        DBOptions dbOptions = new DBOptions(options);

        dbOptions.setCreateIfMissing(true);
        dbOptions.setCreateMissingColumnFamilies(true);
        dbOptions.setMaxBackgroundCompactions(4);
        dbOptions.setDbWriteBufferSize(65 << 20);

        List<ColumnFamilyHandle> columnHandles = new ArrayList<>();
        return RocksDB.open(dbOptions, rocksPath.toString(), columnDescriptors, columnHandles);
    }
}
